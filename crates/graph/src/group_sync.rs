use serde::Deserialize;

use super::client::GraphClient;

#[derive(Debug, Clone)]
pub struct ResolvedGroupMember {
    pub email: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GroupResolutionResult {
    pub members: Vec<ResolvedGroupMember>,
    pub total_count: usize,
    pub resolved_count: usize,
}

#[derive(Debug, Clone)]
pub struct ExchangeGroup {
    pub id: String,
    pub display_name: String,
    pub email: Option<String>,
    pub group_type: ExchangeGroupType,
}

#[derive(Debug, Clone)]
pub enum ExchangeGroupType {
    M365Group,
    DistributionList,
    MailEnabledSecurityGroup,
}

impl ExchangeGroupType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::M365Group => "m365",
            Self::DistributionList => "distribution_list",
            Self::MailEnabledSecurityGroup => "mail_security",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct GraphGroupsResponse {
    pub value: Vec<GraphGroup>,
    #[serde(rename = "@odata.nextLink")]
    pub next_link: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphGroup {
    pub id: String,
    pub display_name: Option<String>,
    pub mail: Option<String>,
    pub group_types: Option<Vec<String>>,
    pub mail_enabled: Option<bool>,
    pub security_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct GraphMembersResponse {
    pub value: Vec<GraphGroupMember>,
    #[serde(rename = "@odata.nextLink")]
    pub next_link: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphGroupMember {
    pub display_name: Option<String>,
    pub mail: Option<String>,
    pub user_principal_name: Option<String>,
}

pub async fn resolve_group_members(
    client: &GraphClient,
    group_id: &str,
) -> Result<GroupResolutionResult, String> {
    let enc_id = urlencoding::encode(group_id);
    let initial_url = format!(
        "/groups/{enc_id}/transitiveMembers/microsoft.graph.user\
         ?$select=displayName,mail,userPrincipalName&$top=999"
    );

    let mut all_members = Vec::new();
    let mut next_link: Option<String> = None;

    loop {
        let page: GraphMembersResponse = if let Some(ref link) = next_link {
            client.get_absolute(link).await?
        } else {
            client.get_json(&initial_url).await?
        };

        for member in &page.value {
            if let Some(resolved) = extract_member_email(member) {
                all_members.push(resolved);
            }
        }

        match page.next_link {
            Some(link) => next_link = Some(link),
            None => break,
        }
    }

    let total_count = all_members.len();
    Ok(GroupResolutionResult {
        resolved_count: total_count,
        total_count,
        members: all_members,
    })
}

pub async fn fetch_user_groups(client: &GraphClient) -> Result<Vec<ExchangeGroup>, String> {
    let prefix = client.api_path_prefix();
    let initial_url = format!(
        "{prefix}/memberOf/microsoft.graph.group\
         ?$filter=mailEnabled eq true\
         &$select=id,displayName,mail,groupTypes,mailEnabled,securityEnabled\
         &$top=999"
    );

    let mut all_groups = Vec::new();
    let mut next_link: Option<String> = None;

    loop {
        let page: GraphGroupsResponse = if let Some(ref link) = next_link {
            client.get_absolute(link).await?
        } else {
            client.get_json(&initial_url).await?
        };

        for group in &page.value {
            if let Some(classified) = classify_group(group) {
                all_groups.push(classified);
            }
        }

        match page.next_link {
            Some(link) => next_link = Some(link),
            None => break,
        }
    }

    Ok(all_groups)
}

fn classify_group(group: &GraphGroup) -> Option<ExchangeGroup> {
    let mail_enabled = group.mail_enabled.unwrap_or(false);
    if !mail_enabled {
        return None;
    }

    let group_types = group.group_types.as_deref().unwrap_or(&[]);
    let security_enabled = group.security_enabled.unwrap_or(false);

    let group_type = if group_types.iter().any(|t| t == "Unified") {
        ExchangeGroupType::M365Group
    } else if security_enabled {
        ExchangeGroupType::MailEnabledSecurityGroup
    } else {
        ExchangeGroupType::DistributionList
    };

    Some(ExchangeGroup {
        id: group.id.clone(),
        display_name: group
            .display_name
            .clone()
            .unwrap_or_else(|| "Unnamed Group".to_string()),
        email: group.mail.clone(),
        group_type,
    })
}

fn extract_member_email(member: &GraphGroupMember) -> Option<ResolvedGroupMember> {
    let email = member
        .mail
        .as_deref()
        .filter(|m| !m.is_empty())
        .or_else(|| {
            member
                .user_principal_name
                .as_deref()
                .filter(|u| !u.is_empty())
        })?;

    Some(ResolvedGroupMember {
        email: email.to_lowercase(),
        display_name: member.display_name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_m365_group() {
        let group = GraphGroup {
            id: "g1".to_string(),
            display_name: Some("Engineering".to_string()),
            mail: Some("eng@contoso.com".to_string()),
            group_types: Some(vec!["Unified".to_string()]),
            mail_enabled: Some(true),
            security_enabled: Some(false),
        };
        let result = classify_group(&group).expect("should classify");
        assert_eq!(result.display_name, "Engineering");
        assert!(matches!(result.group_type, ExchangeGroupType::M365Group));
    }

    #[test]
    fn classify_distribution_list() {
        let group = GraphGroup {
            id: "g2".to_string(),
            display_name: Some("All Staff".to_string()),
            mail: Some("allstaff@contoso.com".to_string()),
            group_types: Some(vec![]),
            mail_enabled: Some(true),
            security_enabled: Some(false),
        };
        let result = classify_group(&group).expect("should classify");
        assert!(matches!(
            result.group_type,
            ExchangeGroupType::DistributionList
        ));
    }

    #[test]
    fn classify_mail_enabled_security_group() {
        let group = GraphGroup {
            id: "g3".to_string(),
            display_name: Some("Security Team".to_string()),
            mail: Some("sec@contoso.com".to_string()),
            group_types: Some(vec![]),
            mail_enabled: Some(true),
            security_enabled: Some(true),
        };
        let result = classify_group(&group).expect("should classify");
        assert!(matches!(
            result.group_type,
            ExchangeGroupType::MailEnabledSecurityGroup
        ));
    }

    #[test]
    fn exclude_security_only_group() {
        let group = GraphGroup {
            id: "g4".to_string(),
            display_name: Some("Admins".to_string()),
            mail: None,
            group_types: Some(vec![]),
            mail_enabled: Some(false),
            security_enabled: Some(true),
        };
        assert!(classify_group(&group).is_none());
    }

    #[test]
    fn extract_email_prefers_mail() {
        let member = GraphGroupMember {
            display_name: Some("Alice".to_string()),
            mail: Some("Alice@Contoso.com".to_string()),
            user_principal_name: Some("alice@contoso.onmicrosoft.com".to_string()),
        };
        let result = extract_member_email(&member).expect("should extract");
        assert_eq!(result.email, "alice@contoso.com");
        assert_eq!(result.display_name.as_deref(), Some("Alice"));
    }

    #[test]
    fn extract_email_falls_back_to_upn() {
        let member = GraphGroupMember {
            display_name: Some("Bob".to_string()),
            mail: None,
            user_principal_name: Some("Bob@Contoso.com".to_string()),
        };
        let result = extract_member_email(&member).expect("should extract");
        assert_eq!(result.email, "bob@contoso.com");
    }

    #[test]
    fn extract_email_none_when_both_missing() {
        let member = GraphGroupMember {
            display_name: Some("Ghost".to_string()),
            mail: None,
            user_principal_name: None,
        };
        assert!(extract_member_email(&member).is_none());
    }

    #[test]
    fn extract_email_skips_empty_mail() {
        let member = GraphGroupMember {
            display_name: None,
            mail: Some(String::new()),
            user_principal_name: Some("user@contoso.com".to_string()),
        };
        let result = extract_member_email(&member).expect("should fall back to UPN");
        assert_eq!(result.email, "user@contoso.com");
    }

    #[test]
    fn deserialize_graph_group() {
        let json = r#"{
            "id": "abc-123",
            "displayName": "Test Group",
            "mail": "test@example.com",
            "groupTypes": ["Unified"],
            "mailEnabled": true,
            "securityEnabled": false
        }"#;
        let group: GraphGroup = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(group.id, "abc-123");
        assert_eq!(group.display_name.as_deref(), Some("Test Group"));
        assert_eq!(group.mail.as_deref(), Some("test@example.com"));
        assert!(
            group
                .group_types
                .as_ref()
                .is_some_and(|t| t.contains(&"Unified".to_string()))
        );
        assert_eq!(group.mail_enabled, Some(true));
        assert_eq!(group.security_enabled, Some(false));
    }

    #[test]
    fn deserialize_graph_member() {
        let json = r#"{
            "displayName": "Jane Doe",
            "mail": "jane@example.com",
            "userPrincipalName": "jane@example.onmicrosoft.com"
        }"#;
        let member: GraphGroupMember = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(member.display_name.as_deref(), Some("Jane Doe"));
        assert_eq!(member.mail.as_deref(), Some("jane@example.com"));
    }

    #[test]
    fn classify_group_with_no_group_types() {
        let group = GraphGroup {
            id: "g5".to_string(),
            display_name: Some("Legacy DL".to_string()),
            mail: Some("legacydl@contoso.com".to_string()),
            group_types: None,
            mail_enabled: Some(true),
            security_enabled: Some(false),
        };
        let result = classify_group(&group).expect("should classify");
        assert!(matches!(
            result.group_type,
            ExchangeGroupType::DistributionList
        ));
    }

    #[test]
    fn group_type_as_str() {
        assert_eq!(ExchangeGroupType::M365Group.as_str(), "m365");
        assert_eq!(
            ExchangeGroupType::DistributionList.as_str(),
            "distribution_list"
        );
        assert_eq!(
            ExchangeGroupType::MailEnabledSecurityGroup.as_str(),
            "mail_security"
        );
    }
}
