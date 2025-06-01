mod lexer_functions;
mod database;
mod container;
mod parser;
mod strix;
mod row;
mod query;
mod indexing;
mod alba_types;
mod query_conditions;
use std::io::{Error,ErrorKind};
use alba_types::AlbaTypes;
use tokio;
use database::connect;
use lexer_functions::{
    lexer_boolean_match, lexer_bytes_match, lexer_group_match, lexer_ignore_comments_match, lexer_keyword_match, lexer_number_match, lexer_operator_match, lexer_string_match, lexer_subcommand_match, Token
};
pub mod better_logs;

fn lexer(input: String) -> Result<Vec<Token>, Error> {
    if input.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank".to_string()));
    }

    let mut characters = input.trim().chars().peekable();
    let mut result = Vec::with_capacity(20);
    let mut dough = String::new();

    while let Some(c) = characters.next() {
        if c == '?'{
            result.push(Token::Argument);
            continue;
        }
        dough.push(c);

        lexer_ignore_comments_match(&mut dough, &mut characters);
        lexer_keyword_match(&mut result, &mut dough);
        lexer_subcommand_match(&mut result, &mut dough, &mut characters)?;
        lexer_group_match(&mut result, &mut dough, &mut characters);
        lexer_boolean_match(&mut result, &mut dough, &mut characters);
        lexer_number_match(&mut result, &mut dough, &mut characters);
        lexer_operator_match(&mut result, &mut dough, &mut characters);
        lexer_string_match(&mut result, &mut dough, &mut characters);
        lexer_bytes_match(&mut result, &mut dough, &mut characters);
    }

    if !dough.trim().is_empty() {
        lexer_keyword_match(&mut result, &mut dough);
        lexer_subcommand_match(&mut result, &mut dough, &mut characters)?;
        lexer_group_match(&mut result, &mut dough, &mut characters);
        lexer_boolean_match(&mut result, &mut dough, &mut characters);
        lexer_operator_match(&mut result, &mut dough, &mut characters);
        lexer_number_match(&mut result, &mut dough, &mut characters);
        lexer_string_match(&mut result, &mut dough, &mut characters);
        lexer_bytes_match(&mut result, &mut dough, &mut characters);
    }

    if !dough.trim().is_empty() {
        result.push(Token::String(dough))
    }

    if result.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "The given input did not produced tokens".to_string()));
    }

    Ok(result)
}

/*



- CREATE <Instance> ...
| CREATE CONTAINER <name> [col_nam][col_typ] 
| CREATE ROW [col_nam][col_val] ON <container:name>

- EDIT <Instance> ...
| EDIT ROW [col_name][col_val] ON <container:name> WHERE <conditions>

- DELETE <instance> ...
| DELETE ROW ON <container> WHERE <conditions>
| DELETE ROW ON <container>
| DELETE CONTAINER <container>

- SEARCH <col_nam> ON <container> ... 
| SEARCH <col_nam> ON <container>
| SEARCH <col_nam> ON <container> WHERE <conditions>

*/
#[derive(Debug, Clone, PartialEq)]
enum AST{
    CreateContainer(AstCreateContainer),
    CreateRow(AstCreateRow),
    EditRow(AstEditRow),
    DeleteRow(AstDeleteRow),
    DeleteContainer(AstDeleteContainer),
    Search(AstSearch),
    Commit(AstCommit),
    Rollback(AstRollback),
    QueryControlNext(AstQueryControlNext),
    QueryControlPrevious(AstQueryControlPrevious),
    QueryControlExit(AstQueryControlExit),
}



#[derive(Debug, Clone, PartialEq)]
struct AstCreateContainer{
    name : String,
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstCreateRow{
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
    container : String
}
#[derive(Debug, Clone, PartialEq)]
struct AstEditRow{
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
    container : String,
    conditions : (Vec<(Token,Token,Token)>,Vec<(usize,char)>)
}
#[derive(Debug, Clone, PartialEq)]
struct AstDeleteRow{
    container : String,
    conditions : Option<(Vec<(Token,Token,Token)>,Vec<(usize,char)>)>
}
#[derive(Debug, Clone, PartialEq)]
struct AstDeleteContainer{
    container : String,
}

#[derive(Debug, Clone, PartialEq)]
enum AlbaContainer {
    Real(String),
    Virtual(Vec<Token>)
}

#[derive(Debug, Clone, PartialEq)]
struct AstSearch{
    container : Vec<AlbaContainer>,
    conditions : (Vec<(Token,Token,Token)>,Vec<(usize,char)>),
    col_nam : Vec<String>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstCommit{
    container : Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct AstRollback{
    container : Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstQueryControlNext{
    id : String,
}
#[derive(Debug, Clone, PartialEq)]
struct AstQueryControlPrevious{
    id : String,
}
#[derive(Debug, Clone, PartialEq)]
struct AstQueryControlExit{
    id : String,
}

fn gerr(msg : &str) -> Error{
    return Error::new(ErrorKind::Other, msg.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = match connect().await{
        Ok(database) => database,
        Err(e) => panic!("{}",e.to_string())
    };
    if let Err(e) = db.run_database().await{
        logerr!("{}",e);
    };
    Ok(())
}