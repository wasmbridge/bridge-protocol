use std::fs;
use std::path::PathBuf;

pub struct CertPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
}

pub fn get_cert_paths() -> CertPaths {
    let base_dir = if cfg!(target_os = "windows") {
        let mut path = PathBuf::from(std::env::var("APPDATA").unwrap_or_else(|_| ".".to_string()));
        path.push("WasmBridgeCloud");
        path.push("certs");
        path
    } else {
        PathBuf::from("./certs")
    };

    if !base_dir.exists() {
        fs::create_dir_all(&base_dir).ok();
    }

    CertPaths {
        cert: base_dir.join("cert.pem"),
        key: base_dir.join("key.pem"),
    }
}

pub fn ensure_certificates() -> Result<CertPaths, Box<dyn std::error::Error + Send + Sync>> {
    let paths = get_cert_paths();

    if !paths.cert.exists() || !paths.key.exists() {
        println!("[CloudBridge] Generating self-signed certificate...");
        let cert = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])?;

        fs::write(&paths.cert, cert.cert.pem())?;
        fs::write(&paths.key, cert.key_pair.serialize_pem())?;
        println!("[CloudBridge] Certificates saved to: {:?}", paths.cert);
    }

    Ok(paths)
}
