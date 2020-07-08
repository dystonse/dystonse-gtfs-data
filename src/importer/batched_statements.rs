use mysql::prelude::*;
use mysql::*;
use crate::FnResult;
use std::sync::Mutex;

const MAX_BATCH_SIZE: usize = 1000;

/// This struct lets you execute multiple SQL statements for multiple parameter sets
/// wihtin a single transaction.
/// 
/// When you create a BatchedStatements instance, you provide one or more statements.
/// Then you call add_paramter_set several times. The struct will collect the parameters.
/// Whenever there would be more collected parameter_sets than MAX_BATCH_SIZE,
/// they will be written to the database within the call to add_paramter_set.AccessMode
/// 
/// When finished, you have to call write_to_database to handle the leftover parameter_sets.
pub struct BatchedStatements {
    params_vec_mutex: Mutex<Vec<Params>>,
    conn_mutex: Mutex<PooledConn>,
    statements: Vec<Statement>,
}

impl<'a> BatchedStatements {
    pub fn new(conn: PooledConn, statements: Vec<Statement>) -> Self {
        BatchedStatements {
            params_vec_mutex: Mutex::new(Vec::with_capacity(MAX_BATCH_SIZE)),
            conn_mutex: Mutex::new(conn),
            statements
        }
    }

    pub fn add_paramter_set(&self, paramter_set: Params) -> FnResult<()> {
        let mut params_vec = self.params_vec_mutex.lock().unwrap();
        params_vec.push(paramter_set);
        println!("Add params, len is now: {}.", params_vec.len());
        if params_vec.len() >= MAX_BATCH_SIZE {
            self.write_to_database_internal(params_vec)?;
        }
        Ok(())
    }

    fn write_to_database_internal(&self, mut params_vec: std::sync::MutexGuard<Vec<Params>>) -> FnResult<()> {
        let mut conn = self.conn_mutex.lock().unwrap();
        let mut tx = conn.start_transaction(TxOpts::default())?;
        println!("Have {} records to write.", params_vec.len());
        for statement in &self.statements {
            tx.exec_batch(statement, params_vec.iter())?;
        }
        params_vec.clear();
        tx.commit()?;
        Ok(())
    }

    pub fn write_to_database(&self) -> FnResult<()> {
        let params_vec = self.params_vec_mutex.lock().unwrap();
        self.write_to_database_internal(params_vec)
    }
}
