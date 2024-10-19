use blake3::Hasher;
use std::fs;
use log::{info, trace, warn};


#[derive(Debug, serde::Deserialize)]
struct Config {
    hostname: String,
    user: String,
    password: String,
}

#[tokio::main]
async fn main() {
    // let config_path = xdg::BaseDirectories::with_prefix("chcache")
    //     .unwrap()
    //     .place_config_file("config.toml")
    //     .unwrap();
    //
    // if !config_path.exists() {
    //     panic!("Config file not found at {}", config_path.display());
    // }

    // let config = fs::read_to_string(config_path).expect("Missing config file?");
    // let config: Config = toml::from_str(&config).expect("Unable to load config, is it a valid toml?");
    let config: Config = Config {
        hostname: std::env::var("CH_HOSTNAME").unwrap(),
        user: std::env::var("CH_USER").unwrap(),
        password: std::env::var("CH_PASSWORD").unwrap(),
    };

    env_logger::init();

    compiler_cache_entrypoint(&config).await;
}

fn assume_base_path(args: &Vec<String>) -> String {
    let cwd: String = match std::env::current_dir() {
        Ok(pathbuf) => pathbuf.into_os_string().into_string().unwrap(),
        _ => {
            panic!("Couldn't get current directory");
        }
    };

    let mut maybe_basepath: Vec<String> = vec![cwd];

    for (i, arg) in args.iter().enumerate() {
        if arg.starts_with("-I") {
            maybe_basepath.push(arg[2..].to_string());
            continue;
        }
        if arg == "-isystem" || arg == "-c" {
            maybe_basepath.push(args[i + 1].to_string());
            continue;
        }
        if arg.starts_with("--gcc-toolchain") {
            maybe_basepath.push(arg[16..].to_string());
            continue;
        }
        if arg.starts_with("--sysroot") {
            maybe_basepath.push(arg[10..].to_string());
            continue;
        }
    }

    let maybe_basepaths_sep_by_slash: Vec<Vec<String>> = maybe_basepath
        .into_iter()
        .map(|x| x.split("/").map(|x| x.to_string()).collect())
        .collect();
    let mut basepath = "".to_string();

    for i in 0..maybe_basepaths_sep_by_slash[0].len() {
        for j in 1..maybe_basepaths_sep_by_slash.len() {
            if maybe_basepaths_sep_by_slash[0][i] != maybe_basepaths_sep_by_slash[j][i] {
                return basepath.trim_end_matches('/').to_string();
            }
        }
        basepath.push_str(&maybe_basepaths_sep_by_slash[0][i]);
        basepath.push_str("/");
    }

    basepath.trim_end_matches('/').to_string()
}

fn compiler_version(compiler: String) -> String {
    // let find_compiler_vars = vec![
    //     "CXX",
    //     "CC",
    // ];
    //
    // let compiler_from_env = find_compiler_vars
    //     .iter()
    //     .map(|x| std::env::var(x))
    //     .find(|x| x.is_ok())
    //     .unwrap_or_else(|| Ok(String::from("clang")))
    //     .unwrap();

    trace!("Using compiler: {}", compiler);

    let compiler_version = std::process::Command::new(compiler)
        .arg("-dM")
        .arg("-E")
        .arg("-x")
        .arg("c")
        .arg("/dev/null")
        .output()
        .expect("Failed to execute command");

    let compiler_version = String::from_utf8_lossy(&compiler_version.stdout);

    let compiler_version = compiler_version
        .lines()
        .find(|x| x.starts_with("#define __VERSION__"))
        .unwrap()
        .split_whitespace()
        .skip(2)
        .collect::<Vec<&str>>()
        .join(" ");

    compiler_version.trim_matches('"').to_string()
}

fn get_output_from_args(args: &Vec<String>) -> String {
    let mut target = String::new();
    for (i, arg) in args.iter().enumerate() {
        if arg == "-o" {
            target = args[i + 1].to_string();
            break;
        }
    }
    target
}

fn get_input_from_args(args: &Vec<String>) -> String {
    let mut target = String::new();
    for (i, arg) in args.iter().enumerate() {
        if arg == "-c" {
            target = args[i + 1].to_string();
            break;
        }
    }
    target
}

fn hash_preprocessed_compiler_output(compiler: String, args: &Vec<String>) -> String {
    let mut preprocess_args = vec![
        "-E".to_string(),
        "-P".to_string(),
        "-fminimize-whitespace".to_string(),
    ];
    preprocess_args.extend(args.clone());

    let output_flag_index = preprocess_args.iter().position(|x| x == "-o").unwrap();
    preprocess_args.remove(output_flag_index);
    preprocess_args.remove(output_flag_index);

    let output = std::process::Command::new(compiler)
        .args(preprocess_args)
        .output()
        .expect("Failed to execute command");
    let output = String::from_utf8_lossy(&output.stdout);

    assert!(output.len() > 0);

    let mut hasher = Hasher::new();
    hasher.update(output.as_bytes());

    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

fn hash_compiler_target(args: &Vec<String>, stripped_args: &Vec<String>) -> String {
    let target: String = get_input_from_args(args);
    let mut hasher = Hasher::new();

    let data = fs::read(&target).expect("Unable to read file");
    hasher.update(&data);

    let target = get_output_from_args(stripped_args);
    hasher.update(&target.as_bytes());

    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

fn path_from_hash(hash: &String) -> String {
    return format!(
        "{}/{}/{}",
        hash.get(0..2).unwrap(),
        hash.get(2..4).unwrap(),
        hash
    );
}

fn get_from_fscache(hash: &String) -> Option<Vec<u8>> {
    let cache_file = xdg::BaseDirectories::with_prefix("chcache")
        .unwrap()
        .get_cache_file(path_from_hash(hash));

    trace!("Cache file: {:?}", cache_file);

    if cache_file.exists() {
        let data = fs::read(cache_file).expect("Unable to read file");
        return Some(data);
    }

    None
}

fn write_to_fscache(hash: &String, data: &Vec<u8>) {
    let cache_file = xdg::BaseDirectories::with_prefix("chcache")
        .unwrap()
        .place_cache_file(path_from_hash(hash))
        .unwrap();

    if cache_file.exists() {
        return;
    }

    let cache_file_dir = cache_file.parent().unwrap();

    if !cache_file_dir.exists() {
        fs::create_dir_all(cache_file_dir).expect("Unable to create directory");
    }

    fs::write(cache_file, data).expect("Unable to write file");
}

async fn load_from_clickhouse(
    client: &clickhouse::Client,
    hash: &String,
    compiler_version: &String,
) -> Result<Option<Vec<u8>>, clickhouse::error::Error> {
    let mut cursor = client
        .query("SELECT ?fields FROM default.build_cache WHERE hash = ? and compiler_version = ? LIMIT 1")
        .bind(hash)
        .bind(compiler_version)
        .fetch::<MyRow>()
        .unwrap();

    while let Some(row) = cursor.next().await? {
        return Ok(Some(row.blob.into()));
    }

    Ok(None)
}

#[derive(Debug, clickhouse::Row, serde::Serialize, serde::Deserialize)]
struct MyRow {
    blob: Vec<u8>,
    hash: String,
    compiler_version: String,
}

async fn load_to_clickhouse(
    client: &clickhouse::Client,
    hash: &String,
    compiler_version: &String,
    data: &Vec<u8>,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert("default.build_cache").unwrap();

    let row = MyRow {
        blob: data.clone(),
        hash: hash.clone(),
        compiler_version: compiler_version.clone(),
    };

    insert.write(&row).await.unwrap();
    insert.end().await
}

async fn compiler_cache_entrypoint(config: &Config) {
    let compiler: String = std::env::args().nth(1).unwrap();
    let rest_of_args: Vec<String> = std::env::args().skip(2).collect();

    trace!("Compiler: {}", compiler);
    // assert!(compiler.contains("clang") || compiler.contains("clang++"));

    trace!("Args: {:?}", rest_of_args);

    let assumed_base_path = assume_base_path(&rest_of_args);
    trace!("Assumed base path: {}", assumed_base_path);

    let stripped_args = rest_of_args
        .iter()
        .map(|x| x.replace(&assumed_base_path, "/"))
        .collect::<Vec<String>>();

    let mut hasher = Hasher::new();
    stripped_args.iter().map(|x| x.as_bytes()).for_each(|x| {
        hasher.update(&x);
    });
    let args_hash = hasher.finalize().to_string();

    let compiler_version = compiler_version(compiler.clone());

    trace!("Compiler version: {}", compiler_version);

    let compiler_target_hash = hash_compiler_target(&rest_of_args, &stripped_args);
    let preprocessed_output_hash =
        hash_preprocessed_compiler_output(compiler.clone(), &rest_of_args);

    trace!("Compiler target hash: {}", compiler_target_hash);
    trace!("Preprocessed output hash: {}", preprocessed_output_hash);
    trace!("Args hash: {}", args_hash);

    hasher.reset();

    hasher.update(compiler_version.as_bytes());
    hasher.update(compiler_target_hash.as_bytes());
    hasher.update(preprocessed_output_hash.as_bytes());
    hasher.update(args_hash.as_bytes());

    let total_hash = hasher.finalize().to_string();
    trace!("Total hash: {}", total_hash);

    let client = clickhouse::Client::default()
        .with_url(&config.hostname)
        .with_user(&config.user)
        .with_password(&config.password)
        .with_compression(clickhouse::Compression::Lz4)
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0");

    let mut did_load_from_cache = false;

    let compiled_bytes: Vec<u8> = match get_from_fscache(&total_hash) {
        Some(bytes) => {
            info!("Local cache hit");
            did_load_from_cache = true;

            fs::write(get_output_from_args(&rest_of_args), &bytes).expect("Unable to write file");

            bytes
        }
        None => {
            trace!("Cache miss");

            let compiled_bytes =
                match load_from_clickhouse(&client, &total_hash, &compiler_version).await {
                    Ok(Some(bytes)) => {
                        did_load_from_cache = true;
                        info!("Loaded from ClickHouse");

                        fs::write(get_output_from_args(&rest_of_args), &bytes)
                            .expect("Unable to write file");

                        bytes
                    }
                    Ok(None) | Err(_) => {
                        let output = std::process::Command::new(compiler)
                            .args(&rest_of_args)
                            .output()
                            .unwrap();
                        if output.status.code().unwrap() != 0 {
                            println!("{}", String::from_utf8_lossy(&output.stdout));
                            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
                            return;
                        }
                        fs::read(get_output_from_args(&rest_of_args)).expect("Unable to read file")
                    }
                };

            compiled_bytes
        }
    };

    write_to_fscache(&total_hash, &compiled_bytes);
    if !did_load_from_cache {
        loop {
            let upload_result =
                load_to_clickhouse(&client, &total_hash, &compiler_version, &compiled_bytes).await;
            if upload_result.is_ok() {
                info!("Uploaded to ClickHouse");
                break;
            }
            warn!("Failed to upload to ClickHouse, retrying...");
        }
    }
}
