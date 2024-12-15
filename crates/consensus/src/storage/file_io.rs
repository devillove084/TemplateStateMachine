use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use opendal::{Operator, Scheme, services::MemoryConfig};
use url::Url;

fn memory_config_build() -> Result<Operator> {
    Ok(Operator::from_config(MemoryConfig::default())?.finish())
}

/// The storage carries all supported storage services in iceberg
#[derive(Debug)]
pub(crate) enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory(Operator),
}

impl Storage {
    fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let (scheme_str, _) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            Scheme::Memory => Ok(Self::Memory(memory_config_build()?)),
            _ => panic!("not supported"),
        }
    }

    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok((op.clone(), stripped))
                } else {
                    Ok((op.clone(), &path[1..]))
                }
            }
        }
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            _ => panic!("not supported"),
        }
    }
}

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
///
/// Supported storages:
///
/// | Storage            | Feature Flag     | Schemes    |
/// |--------------------|-------------------|------------|
/// | Local file system  | `storage-fs`      | `file`     |
/// | Memory             | `storage-memory`  | `memory`   |
/// | S3                 | `storage-s3`      | `s3`, `s3a`|
/// | GCS                | `storage-gcs`     | `gcs`       |
#[derive(Clone, Debug)]
pub struct FileIO {
    inner: Arc<Storage>,
}

impl FileIO {
    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> Result<FileIOBuilder> {
        let url = Url::parse(path.as_ref()).expect("url not ok");
        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        Ok(op.delete(relative_path).await?)
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn remove_all(&self, path: impl AsRef<str>) -> Result<()> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        Ok(op.remove_all(relative_path).await?)
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        Ok(op.exists(relative_path).await?)
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        let path = path.as_ref().to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(InputFile {
            op,
            path,
            relative_path_pos,
        })
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        let path = path.as_ref().to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(OutputFile {
            op,
            path,
            relative_path_pos,
        })
    }

    pub async fn list(&self, prefix: impl AsRef<str>) -> Result<Vec<String>> {
        let (op, relative_prefix) = self.inner.create_operator(&prefix)?;
        let entries = op.list(relative_prefix).await?;
        let paths = entries
            .iter()
            .map(|entry| entry.path().to_string())
            .collect();
        Ok(paths)
    }
}

/// Builder for [`FileIO`].
#[derive(Debug)]
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    props: HashMap<String, String>,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    /// See [`FileIO`] for supported schemes.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
        }
    }

    pub fn scheme(&self) -> &Option<String> {
        &self.scheme_str
    }

    /// Creates a new builder for local file io.
    // pub fn new_fs_io() -> Self {
    //     Self {
    //         scheme_str: None,
    //         props: HashMap::default(),
    //     }
    // }

    /// Fetch the scheme string.
    ///
    /// The scheme_str will be empty if it's None.
    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.scheme_str.unwrap_or_default(), self.props)
    }

    /// Add argument for operator.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    /// Add argument for operator.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> Result<FileIO> {
        let storage = Storage::build(self)?;
        Ok(FileIO {
            inner: Arc::new(storage),
        })
    }
}

/// The struct the represents the metadata of a file.
///
/// TODO: we can add last modified time, content type, etc. in the future.
pub struct FileMetadata {
    /// The size of the file.
    pub size: u64,
}

/// Trait for reading file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileRead: Send + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> Result<Bytes>;
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    op: Operator,
    // Absolution path of file.
    path: String,
    // Relative path of file to uri, starts at [`relative_path_pos`]
    relative_path_pos: usize,
}

impl InputFile {
    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.op.exists(&self.path[self.relative_path_pos..]).await?)
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> Result<FileMetadata> {
        let meta = self.op.stat(&self.path[self.relative_path_pos..]).await?;

        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    /// Read and returns whole content of file.
    ///
    /// For continues reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> Result<Bytes> {
        Ok(self
            .op
            .read(&self.path[self.relative_path_pos..])
            .await?
            .to_bytes())
    }

    /// Creates [`FileRead`] for continues reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> Result<impl FileRead> {
        Ok(self.op.reader(&self.path[self.relative_path_pos..]).await?)
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(opendal::Writer::close(self).await?)
    }
}

/// Output file is used for writing to files..
#[derive(Debug)]
pub struct OutputFile {
    op: Operator,
    // Absolution path of file.
    path: String,
    // Relative path of file to uri, starts at [`relative_path_pos`]
    relative_path_pos: usize,
}

impl OutputFile {
    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.op.exists(&self.path[self.relative_path_pos..]).await?)
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile {
            op: self.op,
            path: self.path,
            relative_path_pos: self.relative_path_pos,
        }
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continues writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    /// Creates output file for continues writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(
            self.op.writer(&self.path[self.relative_path_pos..]).await?,
        ))
    }
}
