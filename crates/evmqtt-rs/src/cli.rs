//! Thin wrappers from the binary's CLI flags to the [`client::Client`]
//! API. Each entry point opens a short-lived MQTT connection, performs
//! the action, prints a one-line confirmation, and exits.

use crate::client::Client;
use crate::config::MqttConfig;
use anyhow::Result;
use tabled::builder::Builder;
use tabled::settings::Style;

pub async fn list_devices(mqtt: &MqttConfig) -> Result<()> {
    let mut client = Client::connect(mqtt).await?;
    let devices = client.list_devices().await?;
    client.shutdown().await?;

    if devices.is_empty() {
        println!("no devices known to {}", mqtt.host);
        return Ok(());
    }

    let mut builder = Builder::default();
    builder.push_record([
        "slug",
        "enabled",
        "name",
        "unique_id",
        "bus",
        "vendor",
        "product",
        "version",
    ]);
    for d in devices {
        let enabled = match d.enabled {
            Some(true) => "on",
            Some(false) => "off",
            None => "?",
        };
        builder.push_record([
            d.slug,
            enabled.to_string(),
            d.name,
            d.unique_id.unwrap_or_else(|| "-".to_string()),
            format!("{:#06x}", d.bus),
            format!("{:#06x}", d.vendor),
            format!("{:#06x}", d.product),
            format!("{:#06x}", d.version),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
    Ok(())
}

pub async fn enable_devices(mqtt: &MqttConfig, slugs: &[String]) -> Result<()> {
    let client = Client::connect(mqtt).await?;
    for slug in slugs {
        client.enable_device(slug).await?;
        println!("enabled {slug}");
    }
    client.shutdown().await
}

pub async fn disable_devices(mqtt: &MqttConfig, slugs: &[String]) -> Result<()> {
    let client = Client::connect(mqtt).await?;
    for slug in slugs {
        client.disable_device(slug).await?;
        println!("disabled {slug}");
    }
    client.shutdown().await
}

pub async fn remove_devices(mqtt: &MqttConfig, slugs: &[String]) -> Result<()> {
    let client = Client::connect(mqtt).await?;
    for slug in slugs {
        client.remove_device(slug).await?;
        println!("removed {slug}");
    }
    client.shutdown().await
}
