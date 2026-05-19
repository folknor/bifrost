#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::Envelope;

// --- ContentDisposition tests ---

#[test]
fn content_disposition_inline() {
    let disp = ContentDisposition {
        disposition_type: "inline".into(),
        params: vec![],
    };
    assert_eq!(disp.disposition_type, "inline");
    assert!(disp.params.is_empty());
}

#[test]
fn content_disposition_attachment_with_filename() {
    let disp = ContentDisposition {
        disposition_type: "attachment".into(),
        params: vec![("filename".into(), "report.pdf".into())],
    };
    assert_eq!(disp.disposition_type, "attachment");
    assert_eq!(disp.params.len(), 1);
    assert_eq!(disp.params[0], ("filename".into(), "report.pdf".into()));
}

#[test]
fn content_disposition_multiple_params() {
    let disp = ContentDisposition {
        disposition_type: "attachment".into(),
        params: vec![
            ("filename".into(), "doc.txt".into()),
            ("size".into(), "1024".into()),
        ],
    };
    assert_eq!(disp.params.len(), 2);
}

#[test]
fn content_disposition_clone_eq() {
    let disp = ContentDisposition {
        disposition_type: "inline".into(),
        params: vec![("charset".into(), "utf-8".into())],
    };
    let cloned = disp.clone();
    assert_eq!(disp, cloned);
}

// --- BodyStructure::Basic tests ---

#[test]
fn basic_body_structure() {
    let body = BodyStructure::Basic {
        media_type: "image".into(),
        media_subtype: "png".into(),
        params: vec![("name".into(), "photo.png".into())],
        id: Some("<img001@example.com>".into()),
        description: Some("A photo".into()),
        encoding: "base64".into(),
        size: 54321,
        md5: None,
        disposition: Some(ContentDisposition {
            disposition_type: "inline".into(),
            params: vec![],
        }),
        language: None,
        location: None,
    };
    match &body {
        BodyStructure::Basic {
            media_type,
            media_subtype,
            size,
            id,
            description,
            encoding,
            ..
        } => {
            assert_eq!(media_type, "image");
            assert_eq!(media_subtype, "png");
            assert_eq!(*size, 54321);
            assert_eq!(id.as_deref(), Some("<img001@example.com>"));
            assert_eq!(description.as_deref(), Some("A photo"));
            assert_eq!(encoding, "base64");
        }
        _ => panic!("expected Basic variant"),
    }
}

#[test]
fn basic_body_structure_minimal() {
    let body = BodyStructure::Basic {
        media_type: "application".into(),
        media_subtype: "octet-stream".into(),
        params: vec![],
        id: None,
        description: None,
        encoding: "7bit".into(),
        size: 0,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    match &body {
        BodyStructure::Basic {
            id,
            description,
            md5,
            disposition,
            language,
            location,
            ..
        } => {
            assert!(id.is_none());
            assert!(description.is_none());
            assert!(md5.is_none());
            assert!(disposition.is_none());
            assert!(language.is_none());
            assert!(location.is_none());
        }
        _ => panic!("expected Basic variant"),
    }
}

// --- BodyStructure::Text tests ---

#[test]
fn text_body_structure() {
    let body = BodyStructure::Text {
        media_subtype: "plain".into(),
        params: vec![("charset".into(), "utf-8".into())],
        id: None,
        description: None,
        encoding: "quoted-printable".into(),
        size: 1234,
        lines: 42,
        md5: None,
        disposition: None,
        language: Some(vec!["en".into()]),
        location: None,
    };
    match &body {
        BodyStructure::Text {
            media_subtype,
            lines,
            size,
            language,
            ..
        } => {
            assert_eq!(media_subtype, "plain");
            assert_eq!(*lines, 42);
            assert_eq!(*size, 1234);
            assert_eq!(language.as_ref().unwrap(), &vec!["en".to_string()]);
        }
        _ => panic!("expected Text variant"),
    }
}

#[test]
fn text_html_body_structure() {
    let body = BodyStructure::Text {
        media_subtype: "html".into(),
        params: vec![("charset".into(), "iso-8859-1".into())],
        id: None,
        description: None,
        encoding: "base64".into(),
        size: 5678,
        lines: 100,
        md5: Some("d41d8cd98f00b204e9800998ecf8427e".into()),
        disposition: None,
        language: None,
        location: Some("http://example.com/page.html".into()),
    };
    match &body {
        BodyStructure::Text {
            media_subtype,
            md5,
            location,
            ..
        } => {
            assert_eq!(media_subtype, "html");
            assert!(md5.is_some());
            assert_eq!(location.as_deref(), Some("http://example.com/page.html"));
        }
        _ => panic!("expected Text variant"),
    }
}

// --- BodyStructure::Message tests ---

#[test]
fn message_body_structure() {
    let inner_body = BodyStructure::Text {
        media_subtype: "plain".into(),
        params: vec![],
        id: None,
        description: None,
        encoding: "7bit".into(),
        size: 100,
        lines: 5,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    let envelope = Envelope {
        subject: Some("Forwarded message".into()),
        ..Default::default()
    };
    let body = BodyStructure::Message {
        media_subtype: "rfc822".into(),
        params: vec![],
        id: None,
        description: Some("Forwarded".into()),
        encoding: "7bit".into(),
        size: 500,
        envelope: Box::new(envelope),
        body: Box::new(inner_body),
        lines: 20,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    match &body {
        BodyStructure::Message {
            envelope,
            body: inner,
            lines,
            size,
            ..
        } => {
            assert_eq!(envelope.subject.as_deref(), Some("Forwarded message"));
            assert_eq!(*lines, 20);
            assert_eq!(*size, 500);
            // Verify the inner body is a Text variant.
            assert!(matches!(inner.as_ref(), BodyStructure::Text { .. }));
        }
        _ => panic!("expected Message variant"),
    }
}

// --- BodyStructure::Multipart tests ---

#[test]
fn multipart_body_structure_empty_bodies() {
    let body = BodyStructure::Multipart {
        media_subtype: "mixed".into(),
        bodies: vec![],
        params: vec![],
        disposition: None,
        language: None,
        location: None,
    };
    match &body {
        BodyStructure::Multipart {
            media_subtype,
            bodies,
            ..
        } => {
            assert_eq!(media_subtype, "mixed");
            assert!(bodies.is_empty());
        }
        _ => panic!("expected Multipart variant"),
    }
}

#[test]
fn multipart_with_children() {
    let text_part = BodyStructure::Text {
        media_subtype: "plain".into(),
        params: vec![("charset".into(), "utf-8".into())],
        id: None,
        description: None,
        encoding: "7bit".into(),
        size: 200,
        lines: 10,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    let html_part = BodyStructure::Text {
        media_subtype: "html".into(),
        params: vec![("charset".into(), "utf-8".into())],
        id: None,
        description: None,
        encoding: "quoted-printable".into(),
        size: 400,
        lines: 20,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    let body = BodyStructure::Multipart {
        media_subtype: "alternative".into(),
        bodies: vec![text_part, html_part],
        params: vec![("boundary".into(), "----=_Part_123".into())],
        disposition: None,
        language: None,
        location: None,
    };
    match &body {
        BodyStructure::Multipart {
            media_subtype,
            bodies,
            params,
            ..
        } => {
            assert_eq!(media_subtype, "alternative");
            assert_eq!(bodies.len(), 2);
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].0, "boundary");
        }
        _ => panic!("expected Multipart variant"),
    }
}

#[test]
fn nested_multipart() {
    let text = BodyStructure::Text {
        media_subtype: "plain".into(),
        params: vec![],
        id: None,
        description: None,
        encoding: "7bit".into(),
        size: 50,
        lines: 3,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    let attachment = BodyStructure::Basic {
        media_type: "application".into(),
        media_subtype: "pdf".into(),
        params: vec![],
        id: None,
        description: None,
        encoding: "base64".into(),
        size: 10000,
        md5: None,
        disposition: Some(ContentDisposition {
            disposition_type: "attachment".into(),
            params: vec![("filename".into(), "doc.pdf".into())],
        }),
        language: None,
        location: None,
    };
    let inner_multi = BodyStructure::Multipart {
        media_subtype: "mixed".into(),
        bodies: vec![text, attachment],
        params: vec![],
        disposition: None,
        language: None,
        location: None,
    };
    let outer = BodyStructure::Multipart {
        media_subtype: "mixed".into(),
        bodies: vec![inner_multi],
        params: vec![],
        disposition: None,
        language: None,
        location: None,
    };
    match &outer {
        BodyStructure::Multipart { bodies, .. } => {
            assert_eq!(bodies.len(), 1);
            match &bodies[0] {
                BodyStructure::Multipart {
                    bodies: inner_bodies,
                    ..
                } => {
                    assert_eq!(inner_bodies.len(), 2);
                }
                _ => panic!("expected nested Multipart"),
            }
        }
        _ => panic!("expected Multipart variant"),
    }
}

// --- Clone / Eq ---

#[test]
fn body_structure_clone_eq() {
    let body = BodyStructure::Text {
        media_subtype: "plain".into(),
        params: vec![],
        id: None,
        description: None,
        encoding: "7bit".into(),
        size: 10,
        lines: 1,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    let cloned = body.clone();
    assert_eq!(body, cloned);
}

#[test]
fn body_structure_debug() {
    let body = BodyStructure::Basic {
        media_type: "image".into(),
        media_subtype: "jpeg".into(),
        params: vec![],
        id: None,
        description: None,
        encoding: "base64".into(),
        size: 0,
        md5: None,
        disposition: None,
        language: None,
        location: None,
    };
    let debug = format!("{body:?}");
    assert!(debug.contains("Basic"));
    assert!(debug.contains("jpeg"));
}
