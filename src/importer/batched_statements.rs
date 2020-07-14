use mysql::prelude::*;
use mysql::*;
use crate::FnResult;
use std::sync::Mutex;
use chrono::Duration;

const MAX_BATCH_SIZE: usize = 1000;

/// This struct lets you execute multiple SQL statements for multiple parameter sets
/// wihtin a single transaction.
/// 
/// When you create a BatchedStatements instance, you provide one or more statements.
/// Then you call add_paramter_set several times. The struct will collect the parameters.
/// Whenever there would be more collected parameter_sets than MAX_BATCH_SIZE,
/// they will be written to the database within the call to add_paramter_set.
/// 
/// When finished, you have to call write_to_database to handle the leftover parameter_sets.
/// 
/// This struct is thread safe. Multiple threads can call add_paramter_set at once.
/// The thread which reaches the MAX_BATCH_SIZE limit will be blocked until the data 
/// is written, but other threads can continue to call add_paramter_set and will only
/// block if they add another MAX_BATCH_SIZE before the first one is written.
pub struct BatchedStatements {
    name: String,
    params_vec_mutex: Mutex<Vec<Params>>,
    conn_mutex: Mutex<PooledConn>,
    statements: Vec<Statement>,
}

impl<'a> BatchedStatements {
    pub fn new(name: &str, conn: PooledConn, statements: Vec<Statement>) -> Self {
        BatchedStatements {
            name: name.to_string(),
            params_vec_mutex: Mutex::new(Vec::with_capacity(MAX_BATCH_SIZE)),
            conn_mutex: Mutex::new(conn),
            statements
        }
    }

    pub fn add_paramter_set(&self, paramter_set: Params) -> FnResult<()> {
        let mut items_to_write: Vec<Params> = Vec::new();

        {
            let mut params_vec = self.params_vec_mutex.lock().unwrap();
            params_vec.push(paramter_set);
            // println!("  *** add_paramter_set");
            if params_vec.len() >= MAX_BATCH_SIZE {
                items_to_write.extend(params_vec.drain(..));
            }
        };

        if !items_to_write.is_empty() {
            self.write_to_database_internal(items_to_write)?;
        }
        
        Ok(())
    }

    fn write_to_database_internal(&self, params_vec: Vec<Params>) -> FnResult<()> {
        let mut conn = self.conn_mutex.lock().unwrap();
        let mut tx = conn.start_transaction(TxOpts::default())?;
        println!("{}: Writing {} rows into the DB.", self.name, params_vec.len());
        for statement in &self.statements {
            println!("{}: Time to exec statement: {}", self.name, Duration::span(|| { tx.exec_batch(statement, params_vec.iter()).expect("db should not fail"); () } ));
        }
        println!("{}: Time to commit: {}", self.name, Duration::span(|| { tx.commit().expect("db should not fail"); () } ));
        Ok(())
    }

    pub fn write_to_database(&self) -> FnResult<()> {
        let mut items_to_write: Vec<Params> = Vec::new();
        {
            let mut params_vec = self.params_vec_mutex.lock().unwrap();
            items_to_write.extend(params_vec.drain(..));
        }

        let ret = self.write_to_database_internal(items_to_write);
        ret
    }
}
