//! Helper functions for reading and writing to the filesystem.
//!
//! Typically, a config file in JSON would be read into the matching struct:
//!      `deserialize_json_from_path(&"/path/to/config.conf");`
//!
//! Similarly, to write a struct back into a file as JSON:
//!     `serialize_json_to_path(&struct, &"/path/to/output.json");`
//!
//! Create output directories automatically at a desired path:
//!      `create_incremented_directory(&"/path/to/output/");`
//!
//! Create a new buffered output file:
//!     `let mut outfile = OutputFile::new(&"/path/to/output/file.jsonl");`
//!
//! Append a JSONL entry to the output file:
//!     `outfile.write_json_line(&json!(struct));`
//!
//! Read a CSV file into vectors of columns (f64):
//!     `let data = deserialize_csv_column_vectors_from_path<f64>(&"/path/to/csv/data.csv")`
//!
//! Read a CSV file into vectors of columns (String):
//!     `let data = deserialize_csv_column_vectors_from_path<String>(&"/path/to/csv/data.csv")`
//!
//! Read a CSV file into a vector of rows (Vec<Vec<f64>>) (Where each row becomes one object):
//!     `let data = deserialize_csv_rows_from_path<Vec<f64>>(&"/path/to/csv/data.csv")`
//!
//! Read a CSV file into a vector of rows (Vec<MyStruct>) (Where each row becomes one object):
//!     `let data = deserialize_csv_rows_from_path<MyStruct>(&"/path/to/csv/data.csv")`
//!

use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use serde_json;
use serde_jsonlines::WriteExt;
use std::clone::Clone;
use std::ffi::OsStr;
use std::fs::{File, Metadata, OpenOptions, read_dir};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
#[error("file: `{path}`")]
pub struct FileIoError<T> {
    path: Box<Path>,
    source: T,
}

#[derive(Debug, Error)]
#[error("error with file")]
pub enum Error {
    #[error("parsing error with CSV file")]
    ParseCsv(#[from] FileIoError<csv::Error>),
    #[error("IO error with file")]
    FileIo(#[from] FileIoError<std::io::Error>),
    #[error("parsing error with JSON file")]
    ParseJson(#[from] serde_json::Error),
    #[error("IO error with file: `{path}`: {msg}")]
    Create { path: Box<Path>, msg: String },
    #[error("invalid file or directory:`{path}`: {msg}")]
    InvalidType { path: Box<Path>, msg: String },
    #[error("IO error with file")]
    Fail(#[from] std::io::Error),
}

/// Wrapper around a buffered file writer, implementing a method to write json lines.
#[derive(Debug)]
pub struct OutputFile {
    writer: BufWriter<File>,
    path: Box<Path>,
}

impl OutputFile {
    /// Create new file for buffered writing of output.
    pub fn new(path: impl AsRef<Path>) -> Result<OutputFile, Error> {
        let path = path.as_ref();
        let writer = create_buffered_file_writer(path)?;

        Ok(OutputFile {
            writer,
            path: Path::new(path).into(),
        })
    }

    /// Appends a JSON line to the `BufWriter`.
    pub fn write_json_line<T>(&mut self, jsonl: &T) -> Result<(), Error>
    where
        T: Serialize,
    {
        self.writer
            .write_json_lines([jsonl])
            .map_err(|source| FileIoError {
                path: self.path.clone(),
                source,
            })?;

        Ok(())
    }

    /// Appends a JSON structure to the `BufWriter`.
    pub fn write_json<T>(&mut self, data: &T) -> Result<(), Error>
    where
        T: Serialize,
    {
        let data = serde_json::to_string_pretty(&data)?;
        write!(self.writer, "{}", &data).map_err(|source| FileIoError {
            path: self.path.clone(),
            source,
        })?;

        Ok(())
    }
}

/// Creates a buffered file for writing at the provided path.
/// # Errors
///
/// Returns an error if:
///
/// *  opening or creating `path` fails.
pub fn create_buffered_file_writer(path: impl AsRef<Path>) -> Result<BufWriter<File>, Error> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|source| FileIoError {
            path: path.as_ref().into(),
            source,
        })?;

    Ok(BufWriter::new(file))
}

/// Create a new directory at the provided path if it doesn't already exist.
///
/// All parents in the path are created as needed.
/// # Errors
///
/// Returns an error if:
/// *  path exists and is not a directory.
/// *  opening or creating `path` fails.
pub fn create_directory(path: impl AsRef<Path>) -> Result<(), Error> {
    let path = path.as_ref();

    // Check if the path exists and is actually a directory (not a special device).
    if let Ok((_, metadata)) = open(path) {
        if metadata.is_dir() {
            Ok(())
        } else {
            Err(Error::InvalidType {
                path: path.into(),
                msg: "attempt to open file as a directory".to_string(),
            })
        }
    } else {
        // Directory path doesn't yet exist, create it.
        std::fs::create_dir_all(path).map_err(|source| FileIoError {
            path: path.into(),
            source,
        })?;
        Ok(())
    }
}

/// Create a new directory at the provided path, with the lowest unused numeric suffix.
///
/// e.g. `path/to/output` produces `path/to_output/run_n`
/// All parents in the path are created as needed.
/// # Errors
///
/// Returns an error if:
/// *  path is not a directory.
/// *  opening or creating `path` fails.
pub fn create_incremented_directory(path: impl AsRef<Path>) -> Result<PathBuf, Error> {
    let path = path.as_ref();
    // Create the base directory.
    create_directory(path)?;
    // This is excessive, but we abort on the first failed `create_dir_all` call.
    for i in 0..usize::MAX {
        // Generate the next run iteration.
        let output_path = path.join(format!("run_{i}"));
        // If it doesn't already exist, try to create it.
        if let Ok(false) = Path::new(&output_path).try_exists() {
            std::fs::create_dir_all(&output_path).map_err(|source| FileIoError {
                path: path.into(),
                source,
            })?;
            return Ok(output_path);
        }
    }
    // Couldn't create the directory, give up.
    Err(Error::Create {
        path: path.into(),
        msg: "unable to create file".to_string(),
    })
}

/// Serialize data from a data object to a new file at provided path.
pub fn serialize_json_to_path<T>(data: &T, path: impl AsRef<Path>) -> Result<(), Error>
where
    T: Serialize,
{
    // Create a new file for the output.
    let mut file = OutputFile::new(path)?;
    // Write the contents of the data object as json to the output file.
    file.write_json(data)?;

    Ok(())
}

/// Deserialize json data from a provided path into appropriate data object.
pub fn deserialize_json_from_path<T>(path: impl AsRef<Path>) -> Result<T, Error>
where
    T: for<'a> Deserialize<'a>,
{
    // Open the file containing the data.
    let file = open_file(&path)?;
    let reader = BufReader::new(file);
    // Read the contents of the file as an instance of the appropriate deserialized structure.
    let out = serde_json::from_reader(reader)?;

    Ok(out)
}

/// Opens a file or directory in read-only mode from provided path.
fn open(path: impl AsRef<Path>) -> Result<(File, Metadata), Error> {
    // Opens file from path
    let file = File::open(&path).map_err(|source| FileIoError {
        path: path.as_ref().into(),
        source,
    })?;

    let metadata = file.metadata()?;

    Ok((file, metadata))
}

// Opens a file in read-only mode from provided path.
/// # Errors
///
/// Returns an error if:
///
/// * The user lacks permissions to perform `metadata` call on `path`.
/// * The user lacks permissions to perform `open()` call on `path`.
/// * `path` does not exist.
/// * `path` is not a file.
pub fn open_file(path: impl AsRef<Path>) -> Result<File, Error> {
    let (file, metadata) = open(&path)?;

    if metadata.is_file() {
        Ok(file)
    } else {
        Err(Error::InvalidType {
            path: path.as_ref().into(),
            msg: "attempt to open directory as a file".to_string(),
        })
    }
}

// Opens a directory in read-only mode from provided path.
/// # Errors
///
/// Returns an error if:
///
/// * The user lacks permissions to perform `metadata` call on `path`.
/// * The user lacks permissions to perform `open()` call on `path`.
/// * `path` does not exist.
/// * `path` is not a directory.
pub fn open_dir(path: impl AsRef<Path>) -> Result<File, Error> {
    let (dir, metadata) = open(&path)?;

    if metadata.is_dir() {
        Ok(dir)
    } else {
        Err(Error::InvalidType {
            path: path.as_ref().into(),
            msg: "attempt to open file as a directory".to_string(),
        })
    }
}

/// Deserialize CSV data from a provided path into a vector.
///
/// Each row of the CSV is deserialized into the user supplied `_data_type`
/// Requires the CSV to be standard, with a header value for each field (matching the `_data_type` if it is struct).
/// # Errors
///
/// Returns an error if:
///
/// *  opening `path` fails.
/// *  serialization fails.
pub fn deserialize_csv_rows_from_path<T>(path: impl AsRef<Path>) -> Result<Vec<T>, Error>
where
    T: for<'a> Deserialize<'a> + Clone,
{
    // Open the file containing the data.
    let file = open_file(&path)?;
    // Setup the reading of the CSV file
    let mut reader = ReaderBuilder::new()
        .has_headers(true) // CSV header is expected.
        .comment(Some(b'#')) // Comment lines start with '#'.
        .flexible(false) // All rows must have the same number of fields.
        .delimiter(b',') // Entries are comma separated (actual CSV).
        .from_reader(file);

    let mut out = vec![];

    // Deserialize the CSV into column vectors.
    for result in reader.deserialize::<T>() {
        let result: T = result.map_err(|source| FileIoError {
            path: path.as_ref().into(),
            source,
        })?;
        out.push(result);
    }

    Ok(out)
}

/// Matrix transposition.
///
/// Returns a transposed copy of the original matrix. Works with slices.
/// Inner values are cloned, so can be expensive.
fn transpose<T>(matrix: &[Vec<T>]) -> Vec<Vec<T>>
where
    T: Clone,
{
    if matrix.is_empty() {
        return matrix.to_vec();
    }
    // Create the empty transposed array.
    let mut transposed = vec![vec![]; matrix[0].len()];
    // Fill the transposed array by copying from the matrix.
    for original_row in matrix {
        for (item, transposed_row) in original_row.iter().zip(&mut transposed) {
            transposed_row.push(item.clone());
        }
    }

    transposed
}

/// Deserialize n-dimensional CSV data from a provided path into nested Vectors.
///
/// Each column vector represents a column of the CSV.
/// Requires the CSV to be standard, with a header value for each field.
/// All fields must be of the same type.
/// The data type must be specified by the caller.
pub fn deserialize_csv_column_vectors_from_path<T>(
    path: impl AsRef<Path>,
) -> Result<Vec<Vec<T>>, Error>
where
    T: for<'a> Deserialize<'a> + Clone,
{
    let new = deserialize_csv_rows_from_path::<Vec<T>>(path)?;

    Ok(transpose(&new))
}

/// Returns a `Vector<PathBuf>` containing all files within the provided directory.
/// # Errors
///
/// Returns an error if:
///
/// *  opening `path` fails.
pub fn collect_files_from_dir_path(path: impl AsRef<Path>) -> Result<Vec<PathBuf>, Error> {
    let mut out = vec![];
    let dir_entries = read_dir(&path).map_err(|source| FileIoError {
        path: path.as_ref().into(),
        source,
    })?;

    for file in dir_entries {
        let file = file.map_err(|source| FileIoError {
            path: path.as_ref().into(),
            source,
        })?;

        // Only process files, not nested directories, etc.
        if file.metadata()?.is_file() {
            out.push(file.path());
        }
    }

    Ok(out)
}

/// Deserializes n-dimensional data from all CSV (".csv") files in a provided directory path into one nested Vector.
pub fn deserialize_csv_rows_from_dir_path<T>(
    path: impl AsRef<Path>,
) -> Result<Vec<Vec<Vec<T>>>, Error>
where
    T: for<'a> Deserialize<'a> + Clone,
{
    collect_files_from_dir_path(path)?
        .iter()
        .filter(|file| file.extension() == Some(OsStr::new("csv")))
        .map(|file| deserialize_csv_column_vectors_from_path::<T>(file))
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_deserialize_csv_rows_from_path() {
        let expected = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], vec![
            7.0, 8.0, 9.0,
        ]];
        let result = deserialize_csv_rows_from_path::<Vec<f64>>("tests/example1.csv").unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    #[should_panic]
    fn test_deserialize_csv_rows_from_path_malformed() {
        // Data values are floats, attempt to serialize to unsigned ints will fail.
        let e = deserialize_csv_rows_from_path::<Vec<u64>>("tests/example1.csv");
        dbg!(&e);
        let _ = e.unwrap();
    }

    #[test]
    #[should_panic]
    fn test_deserialize_csv_rows_from_path_eexist() {
        // Path does not exist.
        let e = deserialize_csv_rows_from_path::<Vec<f64>>("tests/non_exist");
        dbg!(&e);
        let _ = e.unwrap();
    }

    #[test]
    #[should_panic]
    fn test_deserialize_csv_rows_from_path_missing() {
        // Missing field in one row.
        let e = deserialize_csv_rows_from_path::<Vec<f64>>("tests/bad/missing_field.csv");
        dbg!(&e);
        let _ = e.unwrap();
    }

    #[test]
    #[should_panic]
    fn test_deserialize_csv_rows_from_path_nonfile() {
        // `test_dir` is a directory, not a file.
        let e = deserialize_csv_rows_from_path::<Vec<f64>>("tests");
        dbg!(&e);
        let _ = e.unwrap();
    }

    #[test]
    fn test_deserialize_csv_rows_from_dir_path() {
        //
        let e = deserialize_csv_rows_from_dir_path::<f64>("tests");
        dbg!(&e);
        let _ = e.unwrap();
    }

    #[test]
    fn test_deserialize_csv_column_vectors_from_path() {
        let expected = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], vec![
            7.0, 8.0, 9.0,
        ]];

        let expected = transpose(&expected);
        let result = deserialize_csv_column_vectors_from_path::<f64>("tests/example1.csv").unwrap();
        assert_eq!(expected, result);
    }
}
