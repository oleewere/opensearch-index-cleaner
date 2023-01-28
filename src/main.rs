use aiven_rs::service::types_elasticsearch::Index;
use aiven_rs::AivenClient;
use chrono::{NaiveDate, Utc};
use dotenv::dotenv;
use reqwest::{Response, Url};
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::process::exit;
use log::{error, warn};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Report {
    pub name: String,
    pub formatted_size: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Service {
    pub service: String,
    pub rules: Vec<Rule>,
    pub summary_reports: Vec<SummaryReport>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct SummaryReport {
    pattern: String,
    name: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Rule {
    index_pattern: String,
    age_threshold: i64,
    date_pattern: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ServiceResult {
    pub name: String,
    pub size: u64,
    pub success: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ServiceResults {
    pub deletes: Vec<ServiceResult>,
    pub total: u64,
    pub total_remaining: u64,
    pub total_human_readable_msg: String,
    pub failures: u64,
    pub reports: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct NotificationData {
    pub attachments: Vec<Attachment>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Attachment {
    pub color: String,
    pub text: String,
    pub title: String,
    pub title_link: Option<String>,
}

fn sizeof_fmt(mut num: u64) -> String {
    let units = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"];
    for unit in units.iter() {
        if num < 1024 {
            return format!("{}{}{}", num, unit, "B");
        }
        num /= 1024;
    }
    format!("{}Yi{}", num, "B")
}

fn filter_indices_by_pattern<'a>(
    indices: &'a Vec<Index>,
    index_pattern: &'a str,
) -> Vec<&'a Index> {
    let re = fnmatch_regex::glob_to_regex(index_pattern).unwrap();
    let indexes = indices
        .iter()
        .filter(|index| re.is_match(&index.index_name))
        .collect::<Vec<&Index>>();
    return indexes;
}

fn days_between_today_and_date(date_pattern: &str, date_str: &str) -> Result<i64, Box<dyn Error>> {
    let today = Utc::now().date_naive();
    let input_date = match NaiveDate::parse_from_str(date_str, date_pattern) {
        Ok(date_time) => date_time,
        Err(e) => return Err(e.into()),
    };
    let days = (today - input_date).num_days();
    Ok(days)
}

async fn cleanup_service(
    aiven_client: &AivenClient,
    project: &str,
    name: &str,
    dry_run: bool,
    rules: &Vec<Rule>,
    summary_reports: &Vec<SummaryReport>,
) -> ServiceResults {
    let mut total_data_deleted = 0;
    let mut num_failures = 0;
    let es_api = aiven_client.service_elasticsearch();
    let indices = es_api.list_indexes(project, name).await.unwrap().indexes;
    let mut full_index_size = 0;
    for index in &indices {
        let act_index_size = index.size;
        full_index_size += act_index_size;
    }

    let mut reports = Vec::new();
    for sum_report in summary_reports {
        let report_pattern = &sum_report.pattern;
        let report_name = &sum_report.name;
        let indexes = filter_indices_by_pattern(&indices, report_pattern);
        if !indexes.is_empty() {
            let mut partial_full_index_size = 0;
            for index in &indexes {
                let act_partial_index_size = index.size;
                partial_full_index_size += act_partial_index_size;
            }
            reports.push((report_name.to_string(), sizeof_fmt(partial_full_index_size)));
        } else {
            reports.push((report_name.to_string(), sizeof_fmt(0)));
        }
    }
    let mut results: Vec<ServiceResult> = vec![];
    let mut index_already_deleted = Vec::new();
    for rule in rules {
        let index_pattern = &rule.index_pattern;
        let age_threshold = rule.age_threshold;
        let date_pattern = rule
            .date_pattern
            .clone()
            .or(Some("%Y.%m.%d".to_string()))
            .unwrap();
        let indexes = filter_indices_by_pattern(&indices, index_pattern);
        for index in &indexes {
            let index_name = index.index_name.clone();
            if index_already_deleted.contains(&index_name) {
                println!(
                    "Index with name {} has been already deleted based on a previous rule pattern.",
                    index_name
                );
                continue;
            }
            if index_name.starts_with(".") {
                println!("Index with name {} is protected.", index_name);
                continue;
            }
            let index_date_str = &index_name[index_name.len() - 10..];
            let age = days_between_today_and_date(&date_pattern, index_date_str.clone()).unwrap();
            if age > age_threshold {
                let mut result = ServiceResult {
                    name: index_name.to_owned(),
                    size: index.size,
                    success: false,
                };
                if dry_run {
                    println!(
                        "Deleting index {} with size {} bytes (dry-run)",
                        index_name, index.size
                    );
                } else {
                    println!(
                        "Deleting index {} with size {} bytes",
                        index_name, index.size
                    );
                    let del_res = es_api.delete_index(project, name, index_name.as_str()).await;
                    match del_res {
                        Ok(_) => {},
                        Err(err) => {
                            warn!("Aiven error: {}", err);
                            num_failures+=1;
                        },
                    }
                }
                index_already_deleted.push(index_name);
                total_data_deleted += index.size;
                result.success = true;
                results.push(result);
            }
        }
    }
    let total_remaining_size = full_index_size - total_data_deleted;
    let human_readable_total_size = sizeof_fmt(total_data_deleted);
    let human_readable_total_remaining_size = sizeof_fmt(total_remaining_size);
    let msg = format!(
        "Cleanup finished for {} service: {} data has been deleted. (Remaining data size: {})",
        name, human_readable_total_size, human_readable_total_remaining_size
    );
    println!("{}", msg);
    return ServiceResults {
        deletes: results,
        total: total_data_deleted,
        total_remaining: full_index_size - total_data_deleted,
        total_human_readable_msg: msg,
        failures: num_failures,
        reports: reports,
    };
}

async fn send_notification(
    opensearch_cleanup_webhook_url: String,
    all_results: Vec<(String, ServiceResults)>,
    aiven_project: String,
) -> Result<Response, reqwest::Error> {
    let mut short_descriptions = Vec::new();
    let mut all_deleted_indexes = Vec::new();
    let mut has_failures = false;
    let mut all_report_texts = Vec::new();

    for (key, service_result) in all_results {
        let failures = service_result.deletes.iter().filter(|r| !r.success).count();
        if failures > 0 {
            has_failures = true;
        }
        let status = if failures == 0 {
            ":white_check_mark:"
        } else {
            ":x:"
        };
        short_descriptions.push(format!(
            "{} - {}",
            service_result.total_human_readable_msg,
            status
        ));

        for deleted_index in service_result.deletes {
            let status = if deleted_index.success {
                ":white_check_mark:"
            } else {
                ":x:"
            };
            if deleted_index.success {
                all_deleted_indexes.push(format!(
                    "{} - {} ({}) - size: {} bytes",
                    status, deleted_index.name, key, deleted_index.size
                ));
            } else {
                all_deleted_indexes.push(format!("{} - {} ({})", status, deleted_index.name, key));
            }
        }
        let service_report_summary_list = service_result.reports;
        if !service_report_summary_list.is_empty() {
            let mut summary_texts = vec![];
            for service_report_summary in service_report_summary_list {
                summary_texts.push(format!(
                    "{}: {}",
                    service_report_summary.0, service_report_summary.1
                ));
            }
            let report_body = summary_texts.join("\n");
            let report_text = format!("Summary for {} (pre-cleanup):\n{}\n", key, report_body);
            all_report_texts.push(report_text);
        }
    }

    let all_report_texts_value = if !all_report_texts.is_empty() {
        format!("\n\n{}", all_report_texts.join("\n"))
    } else {
        "".to_string()
    };
    let details_text = if !all_deleted_indexes.is_empty() {
        format!("\n\nDetails:\n\n{}", all_deleted_indexes.join("\n"))
    } else {
        "\n\nNot found any old indices by pre-defined rules.".to_string()
    };
    let output_text = format!(
        "{}{}{}",
        short_descriptions.join("\n"),
        all_report_texts_value,
        details_text
    );
    let title = format!("{} - Opensearch index cleanup", aiven_project);
    let color = if has_failures { "#E01E5A" } else { "#2EB67D" };
    let title_link_var =
        env::var("NOTIFICATION_TITLE_LINK").unwrap_or_else(|_| "".to_string());
    let title_link = match !title_link_var.is_empty() {
        true => Some(title_link_var),
        false => None,
    };
    let attachment = Attachment {
        title: title,
        title_link: title_link,
        text: output_text,
        color: color.to_string(),
    };
    let notification_data = NotificationData {
        attachments: vec![attachment],
    };
    let req_str = serde_json::to_string(&notification_data).unwrap();
    let client = reqwest::Client::new();
    let url = Url::parse(opensearch_cleanup_webhook_url.as_str()).unwrap();
    let result = client
        .post(url)
        .header("Content-type", "application/json")
        .body(req_str)
        .send()
        .await;
    return result;
}

async fn cleanup() -> Result<bool, Box<dyn Error>> {
    let rules_file = env::var("RULES_FILE").unwrap_or_else(|_| "".to_string());
    let cleanup_dry_run: bool = env::var("CLEANUP_DRY_RUN")
            .unwrap_or("false".to_string())
            .parse()
            .unwrap();
    let aiven_api_token = env::var("AIVEN_API_TOKEN").unwrap_or_else(|_| "".to_string());
    let aiven_project = env::var("AIVEN_PROJECT").unwrap_or_else(|_| "".to_string());
    let mut file = match File::open(rules_file) {
        Ok(file) => file,
        Err(err) => {
            println!("Error opening file: {}", err);
            return Err(err.into());
        },
    };

    let mut contents = String::new();
    match file.read_to_string(&mut contents) {
        Ok(_) => {}
        Err(err) => {
            println!("Error reading file: {}", err);
            return Err(err.into());
        }
    };

    let service_rules: Vec<Service> = match serde_yaml::from_str(&contents) {
        Ok(rules) => rules,
        Err(err) => {
            println!("Error parsing YAML: {}", err);
            return Err(err.into());
        }
    };
    let aiven_client = AivenClient::from_token("https://api.aiven.io", "v1", &aiven_api_token);
    let mut all_results = vec![];
    for service_obj in service_rules {
        let name = service_obj.service.clone();
        all_results.push((
            name.clone(),
            cleanup_service(
                &aiven_client,
                &aiven_project,
                &name.as_str(),
                cleanup_dry_run,
                &service_obj.rules,
                &service_obj.summary_reports,
            )
            .await,
        ));
    }
    let opensearch_cleanup_webhook_url =
        env::var("NOTIFICATION_WEBHOOK_URL").unwrap_or_else(|_| "".to_string());
    if !cleanup_dry_run && !opensearch_cleanup_webhook_url.is_empty() {
        let res =
            send_notification(opensearch_cleanup_webhook_url, all_results, aiven_project).await;
        match res {
            Ok(res) => {
                if !res.status().is_success() {
                    warn!("Notification response is not successful: {}", res.status().as_str());
                    let t = res.text().await.unwrap();
                    warn!("res: {}", t);
                    return Ok(false)
                }
            },
            Err(err) => {
                error!("Notification error: {}", err);
                return Err(err.into())
            },
        }
    }
    return Ok(true);
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();
    match cleanup().await {
        Ok(_) => exit(0),
        Err(err) => {
            error!("Cleanup process failed with error: {}", err);
            exit(1);
        }
    }
}
