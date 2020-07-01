use mysql::prelude::*;
use mysql::*;
use crate::FnResult;

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
    params_vec: Vec<Params>,
    conn: PooledConn,
    statements: Vec<Statement>,
}

impl<'a> BatchedStatements {
    pub fn new(conn: PooledConn, statements: Vec<Statement>) -> Self {
        BatchedStatements {
            params_vec: Vec::with_capacity(MAX_BATCH_SIZE),
            conn,
            statements
        }
    }

    pub fn add_paramter_set(&mut self, paramter_set: Params) -> FnResult<()> {
        self.params_vec.push(paramter_set);
        if self.params_vec.len() >= MAX_BATCH_SIZE {
            self.write_to_database()?;
        }
        Ok(())
    }

    pub fn write_to_database(&mut self) -> FnResult<()> {
        let mut tx = self.conn.start_transaction(TxOpts::default())?;
        for statement in &self.statements {
            tx.exec_batch(statement, &self.params_vec)?;
        }
        self.params_vec.clear();
        tx.commit()?;
        Ok(())
    }
}
