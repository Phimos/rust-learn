use parquet::column::reader::ColumnReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;
use std::rc::Rc;
// #[derive(Debug, PartialEq, Eq)]
// pub struct TreeNode {
//     pub val: i32,
//     pub left: Option<Rc<RefCell<TreeNode>>>,
//     pub right: Option<Rc<RefCell<TreeNode>>>,
// }

// impl TreeNode {
//     #[inline]
//     pub fn new(val: i32) -> Self {
//         TreeNode {
//             val,
//             left: None,
//             right: None,
//         }
//     }
// }

// pub trait Operator {
//     fn execute(&self) -> f64;
//     fn compute(&self) -> f64;
// }

// #[derive(Debug)]
// pub struct Variable {
//     pub name: String,
//     pub buffer: VecDeque<f64>,
// }

// impl Variable {
//     pub fn new(name: &str) -> Self {
//         Variable {
//             name: name.to_string(),
//             buffer: VecDeque::new(),
//         }
//     }
//     pub fn from_vec(name: &str, vec: Vec<f64>) -> Self {
//         Variable {
//             name: name.to_string(),
//             buffer: vec.into_iter().collect(),
//         }
//     }
// }

// pub struct Add {
//     pub left: Box<dyn Operator>,
//     pub right: Box<dyn Operator>,
// }

// impl Add {
//     fn <T1: Operator+Clone, T2: Operator+Clone> new(left: T1, right: T2) -> Self {
//         Add {
//             left: Box::new(left),
//             right: Box::new(right),
//         }
//     }
// }

fn read_column_from_file(path: &Path, column_name: &str) -> Vec<f64> {
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    println!("{:#?}", metadata);
    let mut result: Vec<f64> = Vec::new();

    let batch_size: usize = 1024 * 1024;

    for i in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i).unwrap();
        let row_group_metadata = metadata.row_group(i);
        let column_index = row_group_metadata
            .columns()
            .iter()
            .position(|c| c.column_descr().name() == column_name)
            .unwrap();
        println!("{}", batch_size);
        let mut column_reader = row_group_reader.get_column_reader(column_index).unwrap();
        match column_reader {
            ColumnReader::DoubleColumnReader(ref mut typed_reader) => {
                let mut remain = row_group_metadata.num_rows() as usize;
                let batch_size = std::cmp::min(batch_size, remain);
                let mut values: Vec<f64> = vec![0.0; batch_size];
                let mut def_levels = vec![0; batch_size];
                let mut rep_levels = vec![0; batch_size];

                println!("{} rows in total", row_group_metadata.num_rows());

                while remain > 0 {
                    let (num_rows, _) = typed_reader
                        .read_batch(
                            batch_size,
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut values,
                        )
                        .expect("Error reading column");

                    // println!("Read {} rows", num_rows);
                    remain -= num_rows;

                    result.extend_from_slice(&values[..num_rows]);
                }
            }
            _ => {
                panic!("Unsupported column type");
            }
        }
        // column_reader.read_batch(8, &mut def_levels, &mut rep_levels, &mut values, false);
    }
    // println!("{:?}", result);
    println!("{} items in result", result.len());
    return result;
}

fn rolling_mean(
    data: &mut impl Iterator<Item = f64>,
    window_size: usize,
) -> impl Iterator<Item = f64> {
    let mut window: VecDeque<f64> = VecDeque::new();
    let mut result: Vec<f64> = Vec::new();
    for i in 0..window_size {
        window.push_back(data.next().unwrap().clone());
        result.push(f64::NAN);
    }
    for i in 0..data.size_hint().0 {
        let mean = window.iter().sum::<f64>() / window.len() as f64;
        result.push(mean);
        window.push_back(data.next().unwrap());
        window.pop_front();
    }
    result.into_iter()
}
fn main() {
    let path = Path::new("/Users/yunchong.gan/Documents/swag/sample.parquet");
    // read_column_from_file(path, "High");
    let result = read_column_from_file(path, "High");
    // let out = result + result;
    println!("{:?}", result);

    println!(
        "{:?}",
        result
            .iter()
            .zip(result.iter())
            .map(|(a, b)| a + b)
            .collect::<Vec<f64>>()
    );

    println!(
        "{:?}",
        rolling_mean(&mut result.into_iter(), 10).collect::<Vec<f64>>()
    );

    // let var = Variable::from_vec("High", result);
    // println!("{:#?}", var);
    // println!("{:?}", out);
}
