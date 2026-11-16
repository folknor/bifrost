//! Code generation for `bifrost-types`.
//!
//! [`closing_account`] walks the `Account` trait declaration and generates the
//! closed-account gate's forwarding impl from it, so a trait method cannot be
//! added without the wrapper forwarding it, and the wrapper can never inherit
//! a default body where the wrapped provider overrides it. Every trait method
//! must carry an `#[admission(..)]` policy; a method without one is a compile
//! error, which is what forces the post-close decision for each new method.
//!
//! The policies:
//!
//! - `#[admission(Operation)]` - gated. The provider call is deferred into the
//!   returned future or stream and runs only if the account is still open at
//!   its first poll; otherwise the result is a refusal stamped with
//!   `AccountOperation::Operation`. Borrowed arguments are cloned so the
//!   deferred call owns them.
//! - `#[admission(classify, Operation)]` - the provider is called at once,
//!   because its `Option` answer classifies the argument and must not change
//!   after close; only the stream inside `Some` is gated at its first poll.
//! - `#[admission(exempt)]` - forwarded unchanged. The return value must
//!   implement the `Exempt` marker, which checks the shape only: the
//!   annotation is the policy.
//! - `#[admission(close)]` - closes admission synchronously, then calls the
//!   provider's `close` synchronously and returns its future.
//!
//! The generated items live in the crate that declares the trait and refer to
//! `crate::closing`, so this macro is only meaningful inside `bifrost-types`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, FnArg, Ident, ItemTrait, LitStr, Pat, ReturnType, Token, TraitItem, TraitItemFn,
    Type, parse_macro_input,
};

enum Policy {
    Gate(Ident),
    Classify(Ident),
    Exempt,
    Close,
}

impl Parse for Policy {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let first: Ident = input.parse()?;
        if first == "exempt" {
            return Ok(Self::Exempt);
        }
        if first == "close" {
            return Ok(Self::Close);
        }
        if first == "classify" {
            let _: Token![,] = input.parse()?;
            return Ok(Self::Classify(input.parse()?));
        }
        Ok(Self::Gate(first))
    }
}

/// Generate the closed-account gate impl for the annotated `Account` trait.
#[proc_macro_attribute]
pub fn closing_account(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(Span::call_site(), "closing_account takes no arguments")
            .to_compile_error()
            .into();
    }
    let mut item = parse_macro_input!(item as ItemTrait);
    match expand(&mut item) {
        Ok(generated) => quote!(#item #generated).into(),
        Err(error) => {
            let error = error.to_compile_error();
            quote!(#item #error).into()
        }
    }
}

fn expand(item: &mut ItemTrait) -> syn::Result<TokenStream2> {
    let trait_ident = item.ident.clone();
    let mut methods = Vec::new();
    let mut exempt = Vec::new();
    let mut errors: Option<syn::Error> = None;
    for trait_item in &mut item.items {
        let TraitItem::Fn(method) = trait_item else {
            continue;
        };
        let policy = match take_policy(&mut method.attrs, &method.sig.ident) {
            Ok(policy) => policy,
            Err(error) => {
                combine(&mut errors, error);
                continue;
            }
        };
        if matches!(policy, Policy::Exempt) {
            exempt.push(LitStr::new(
                &method.sig.ident.to_string(),
                method.sig.ident.span(),
            ));
        }
        match forward(&trait_ident, method, &policy) {
            Ok(tokens) => methods.push(tokens),
            Err(error) => combine(&mut errors, error),
        }
    }
    if let Some(errors) = errors {
        return Err(errors);
    }
    Ok(quote! {
        impl<A> #trait_ident for crate::closing::ClosingAccount<A>
        where
            A: #trait_ident + ?Sized + 'static,
        {
            #(#methods)*
        }

        impl<A: ?Sized> crate::closing::ClosingAccount<A> {
            /// Every `Account` method the gate forwards unchanged after close,
            /// generated from the same trait walk as the forwarding impl.
            pub const EXEMPT_METHODS: &'static [&'static str] = &[#(#exempt),*];
        }
    })
}

fn combine(errors: &mut Option<syn::Error>, error: syn::Error) {
    match errors {
        Some(existing) => existing.combine(error),
        None => *errors = Some(error),
    }
}

fn take_policy(attrs: &mut Vec<Attribute>, name: &Ident) -> syn::Result<Policy> {
    let mut found = None;
    let mut kept = Vec::with_capacity(attrs.len());
    for attr in attrs.drain(..) {
        if attr.path().is_ident("admission") {
            if found.is_some() {
                return Err(syn::Error::new_spanned(attr, "duplicate admission policy"));
            }
            found = Some(attr.parse_args::<Policy>()?);
        } else {
            kept.push(attr);
        }
    }
    *attrs = kept;
    found.ok_or_else(|| {
        syn::Error::new(
            name.span(),
            format!(
                "Account::{name} has no #[admission(..)] policy; every trait method must \
                 state what the closed-account gate does with it"
            ),
        )
    })
}

fn forward(
    trait_ident: &Ident,
    method: &TraitItemFn,
    policy: &Policy,
) -> syn::Result<TokenStream2> {
    let sig = &method.sig;
    let name = &sig.ident;
    let output = match &sig.output {
        ReturnType::Default => quote!(),
        ReturnType::Type(arrow, ty) => quote!(#arrow #ty),
    };
    let where_clause = &sig.generics.where_clause;
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            "generic Account methods are not supported by the closed-account gate",
        ));
    }

    let mut inputs = sig.inputs.iter();
    let receiver = match inputs.next() {
        Some(FnArg::Receiver(receiver)) => receiver,
        _ => {
            return Err(syn::Error::new_spanned(
                sig,
                "Account methods must take a self receiver",
            ));
        }
    };
    let by_arc = receiver.reference.is_none();
    let receiver_tokens = if by_arc {
        let ty = &receiver.ty;
        quote!(self: #ty)
    } else {
        quote!(&self)
    };

    let mut params = Vec::new();
    let mut arg_names = Vec::new();
    let mut arg_types = Vec::new();
    for (index, input) in inputs.enumerate() {
        let FnArg::Typed(typed) = input else {
            return Err(syn::Error::new_spanned(input, "unexpected second receiver"));
        };
        if !matches!(&*typed.pat, Pat::Ident(_) | Pat::Wild(_)) {
            return Err(syn::Error::new_spanned(
                &typed.pat,
                "Account method parameters must be plain bindings",
            ));
        }
        let arg = format_ident!("__arg{index}");
        let ty = &typed.ty;
        params.push(quote!(#arg: #ty));
        arg_names.push(arg);
        arg_types.push((*typed.ty).clone());
    }

    let inner_ref = if by_arc {
        quote!(::std::sync::Arc::clone(&self.inner))
    } else {
        quote!(&*self.inner)
    };
    let body = match policy {
        Policy::Exempt => quote! {
            crate::closing::exempt(<A as #trait_ident>::#name(#inner_ref, #(#arg_names),*))
        },
        Policy::Close => quote! {
            self.admission.close();
            <A as #trait_ident>::#name(#inner_ref, #(#arg_names),*)
        },
        Policy::Classify(operation) => quote! {
            let admission = self.admission.clone();
            let protocol = <A as #trait_ident>::protocol(&*self.inner);
            <A as #trait_ident>::#name(#inner_ref, #(#arg_names),*).map(|stream| {
                crate::closing::gate_constructed_stream(
                    admission,
                    crate::AccountOperation::#operation,
                    protocol,
                    stream,
                )
            })
        },
        Policy::Gate(operation) => {
            let mut captures = Vec::new();
            let mut call_args = Vec::new();
            for (arg, ty) in arg_names.iter().zip(&arg_types) {
                match ty {
                    Type::Reference(reference) => {
                        let owned = format_ident!("{arg}_owned");
                        let elem = &reference.elem;
                        let capture = match &**elem {
                            Type::Slice(_) => quote!(let #owned = <#elem>::to_vec(#arg);),
                            _ => quote!(let #owned = <#elem as ::core::clone::Clone>::clone(#arg);),
                        };
                        captures.push(capture);
                        call_args.push(quote!(&#owned));
                    }
                    _ => call_args.push(quote!(#arg)),
                }
            }
            let call = if by_arc {
                quote!(<A as #trait_ident>::#name(inner, #(#call_args),*))
            } else {
                quote!(<A as #trait_ident>::#name(&*inner, #(#call_args),*))
            };
            quote! {
                let protocol = <A as #trait_ident>::protocol(&*self.inner);
                let inner = ::std::sync::Arc::clone(&self.inner);
                #(#captures)*
                crate::closing::gate(
                    self.admission.clone(),
                    crate::AccountOperation::#operation,
                    protocol,
                    move || #call,
                )
            }
        }
    };

    Ok(quote! {
        #[inline]
        fn #name(#receiver_tokens, #(#params),*) #output #where_clause {
            #body
        }
    })
}
