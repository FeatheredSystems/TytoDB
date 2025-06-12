use std::io::{Error, ErrorKind};

use base64::Engine;

use crate::{alba_types::AlbaTypes, gerr, lexer, lexer_functions::{Token, B64ENGINE}, AlbaContainer, AstCommit, AstCreateContainer, AstCreateRow, AstEditRow, AstRollback, AstSearch, AST};



fn parser_debugger_extract_string(output : &mut String,list : &Vec<Token>,index : usize) -> Option<Error>{
    if let Some(cn) = list.get(index){
        match cn{
            Token::String(s) => {
                output.push_str(s.as_str());
                return None
            },
            _ => { return Some(gerr(&format!("Expected a string token at position {}, but found {:?}", index, cn))); }
        }
    }else{
        return Some(gerr("Missing a string"));
    }
}

fn parser_debugger_extract_group_elstr(output : &mut Vec<String>,list : &Vec<Token>,index : usize) -> Option<Error>{
    if let Some(group) = list.get(index) {
        match group{
            Token::Group(ggggg) => {
                for i in ggggg{
                    match i{
                        Token::String(s) => {output.push(s.clone());},
                        _=>{return Some(gerr("Invalid type, must be a group with only strings inside"));}
                    }
                }
            }
            _ => {return Some(gerr("Invalid type, must be a group with only strings inside"));} 
        }
    }
    None
}
fn parser_debugger_extract_group_albatype(
    output: &mut Vec<AlbaTypes>,
    list: &[Token],
    index: usize
) -> Option<Error> {
    if let Some(token) = list.get(index) {
        match token {
            Token::Group(g) => {
                for item in g {
                    match item {
                        Token::String(s) => {
                            let ty = match s.to_uppercase().as_str() {
                                "INT" => AlbaTypes::Int(0),
                                "BIGINT" => AlbaTypes::Bigint(0),
                                "FLOAT" => AlbaTypes::Float(0.0),
                                "BOOL" => AlbaTypes::Bool(false),
                                "TEXT" => AlbaTypes::Text(String::new()),
                                "NANO-STRING" => AlbaTypes::NanoString(String::new()),
                                "SMALL-STRING" => AlbaTypes::SmallString(String::new()),
                                "MEDIUM-STRING" => AlbaTypes::MediumString(String::new()),
                                "BIG-STRING" => AlbaTypes::BigString(String::new()),
                                "LARGE-STRING" => AlbaTypes::LargeString(String::new()),
                                "NANO-BYTES" => AlbaTypes::NanoBytes(Vec::new()),
                                "SMALL-BYTES" => AlbaTypes::SmallBytes(Vec::new()),
                                "MEDIUM-BYTES" => AlbaTypes::MediumBytes(Vec::new()),
                                "BIG-BYTES" => AlbaTypes::BigSBytes(Vec::new()),
                                "LARGE-BYTES" => AlbaTypes::LargeBytes(Vec::new()),
                                _ => return Some(gerr(&format!("Unknown type: {}", s))),
                            };
                            output.push(ty);
                        },
                        _ => return Some(gerr("Expected string for column type")),
                    }
                }
                None
            },
            _ => Some(gerr("Missing column types group")),
        }
    } else {
        Some(gerr("Missing token for column types"))
    }
}

fn debug_create_command(tokens: &Vec<Token>) -> Result<AST,Error>{
    if let Some(instance) = tokens.get(1){
        match instance{
            Token::Keyword(ist) => {
                match ist.as_str(){
                    "CONTAINER" => {
                        let mut cname : String = String::new();
                        let mut col_name : Vec<String> = Vec::with_capacity(5);
                        let mut col_types : Vec<AlbaTypes> = Vec::with_capacity(5);
                        if let Some(err) = parser_debugger_extract_string(&mut cname,tokens,2){
                            return Err(err)
                        }
                        if let Some(cva) = parser_debugger_extract_group_elstr(&mut col_name, tokens, 3){
                            return Err(cva)
                        }
                        let mut col_name_holder : Vec<String> = Vec::with_capacity(5);
                        for i in col_name.iter(){
                            if col_name_holder.contains(&i){
                                return Err(gerr("Repeated column names"))
                            }
                            col_name_holder.push(i.clone());
                        }
                        drop(col_name_holder);
                        if let Some(bruh) = parser_debugger_extract_group_albatype(&mut col_types, tokens, 4){
                            return Err(bruh)
                        }
                        if col_name.len() != col_types.len(){
                            return Err(gerr("All column names and column types are not matching"))
                        }
                        
                        return Ok(AST::CreateContainer(AstCreateContainer { name: cname, col_nam: col_name, col_val: col_types }))
                    }
                    "ROW" => {
                        let mut col_names : Vec<String> = Vec::with_capacity(5);
                        let mut col_values : Vec<AlbaTypes> = Vec::with_capacity(5);
                        let mut container = String::new();

                        if let Some(cva) = parser_debugger_extract_group_elstr(&mut col_names, tokens, 2){
                            return Err(cva)
                        }
                        if let Some(bruh) = parser_debugger_extract_group_albatype(&mut col_values, tokens, 3){
                            return Err(bruh)
                        }
                        if let Some(cn) = tokens.get(5){
                            match cn{
                                Token::String(s) => {
                                    container = s.clone()
                                },
                                _ => {return Err(gerr("Invalid type, the container name must be a string"));}
                            }
                        }
                        return Ok(AST::CreateRow(AstCreateRow { col_nam: col_names, col_val: col_values, container: container }))
                        
                    },
                    _ => {return Err(gerr("Invalid instance type"))}
                }
            },
            _ => {
                return Err(gerr("Invalid command"));
            }
            
        }
    }
    return Err(gerr("Missing the instance to be created"));
}

fn debug_edit_command(tokens : &Vec<Token>) -> Result<AST,Error> {
    if let Some(instance) = tokens.get(1){
        match instance{
            Token::Keyword(st) => {
                match st.as_str(){
                    "ROW" => {
                        let mut ed_col_name : Vec<String> = Vec::with_capacity(20);
                        let mut ed_col_type : Vec<AlbaTypes> = Vec::with_capacity(20);
                        let mut ed_container : String = String::new();
                        let mut conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>) = (Vec::with_capacity(10), Vec::with_capacity(10));   

                        if let Some(errrrrr) = parser_debugger_extract_group_elstr(&mut ed_col_name, &tokens, 2){
                            return Err(errrrrr)
                        }
                        if let Some(errrrrr) = parser_debugger_extract_group_albatype(&mut ed_col_type, &tokens, 3){
                            return Err(errrrrr)
                        }
                        if let Some(t) = tokens.get(4){
                            if let Token::Keyword(kw) = t{
                                if kw != "ON"{
                                    return Err(gerr("Missing container"))
                                }
                            }
                        }
                        if let Some(errrrrr) = parser_debugger_extract_string(&mut ed_container, &tokens, 5){
                            return Err(errrrrr)
                        }
                        
                        if let Some(tok) = tokens.get(6) {
                            if match tok {
                                Token::Keyword(a) if a.to_uppercase() == "WHERE" => false,
                                _ => true,
                            } {
                                return Err(gerr(&format!(r#"In EDIT ROW command, expected keyword 'WHERE' at position 6, but found {:?}"#, tok)));
                            }

                            if let Some(iterator_of_tokens) = tokens.get(7..) {
                                let mut bushes: Vec<Token> = Vec::new();

                                for i in iterator_of_tokens {
                                    if bushes.len() == 3 {
                                        conditions.0.push((
                                            get_from_bushes_with_safety(&bushes, 0)?,
                                            get_from_bushes_with_safety(&bushes, 1)?,
                                            get_from_bushes_with_safety(&bushes, 2)?,
                                        ));
                                        bushes.clear();

                                        conditions.1.push((
                                            conditions.0.len() - 1,
                                            match i {
                                                Token::Keyword(a) => match a.to_uppercase().as_str() {
                                                    "OR" => 'o',
                                                    "AND" => 'a',
                                                    _ => {
                                                        return Err(gerr(
                                                            "Expected logical operator 'AND' or 'OR' after a condition",
                                                        ))
                                                    }
                                                },
                                                _ => return Err(gerr("Expected logical operator after condition")),
                                            },
                                        ));
                                        continue;
                                    }

                                    match i {
                                        Token::String(_) => match bushes.len() {
                                            0 | 2 => bushes.push(i.clone()),
                                            _ => return Err(gerr("Unexpected string: operator might be missing")),
                                        },
                                        Token::Bool(_) | Token::Int(_) | Token::Float(_) => {
                                            if bushes.len() == 2 {
                                                bushes.push(i.clone())
                                            } else {
                                                return Err(gerr("Unexpected value: condition must follow 'column OP value' pattern"));
                                            }
                                        }
                                        Token::Operator(_) => {
                                            if bushes.len() == 1 {
                                                bushes.push(i.clone());
                                            } else {
                                                return Err(gerr("Unexpected operator: check condition structure"));
                                            }
                                        }
                                        _ => return Err(gerr("Unexpected token in WHERE clause")),
                                    }
                                }

                                if bushes.len() == 3 {
                                    conditions.0.push((
                                        get_from_bushes_with_safety(&bushes, 0)?,
                                        get_from_bushes_with_safety(&bushes, 1)?,
                                        get_from_bushes_with_safety(&bushes, 2)?,
                                    ));
                                }
                            }
                        }

                        return Ok(AST::EditRow(AstEditRow{
                            col_nam:ed_col_name,
                            col_val: ed_col_type,
                            container: ed_container,
                            conditions
                        }))
                    },
                    _ => {
                        return Err(gerr("Invalid instance type"));
                    }
                }
            },
            _ => {
                return Err(gerr("Invalid command"));
            }
        }
    }
    return Err(gerr("Missing the instance to be editted"));
}

fn get_from_bushes_with_safety(bushes : &Vec<Token>,index : usize) -> Result<Token,Error>{
    match bushes.get(index){ Some(a) => Ok(a.clone()), None => return Err(gerr("Missing"))}
}

fn debug_search(tokens: &Vec<Token>) -> Result<AST, Error> {
    let container: Vec<AlbaContainer> = match tokens.get(3) {
        Some(s) => match s {
            Token::Group(a) => {
                let mut containers : Vec<AlbaContainer> = Vec::new();
                for i in a{
                    match i{
                        Token::String(str) => {containers.push(AlbaContainer::Real(str.clone()));},
                        Token::SubCommand(a) => {containers.push(AlbaContainer::Virtual(a.clone()))},
                        _ => {
                            return Err(gerr("..."));
                        }
                    }
                }
                containers
            },
            _ => return Err(gerr("Expected container group as a group at position 3")),
        },
        None => return Err(gerr("Missing container group (expected at position 3)")),
    };

    let mut conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>) =
        (Vec::with_capacity(10), Vec::with_capacity(10));

    let columns: Vec<String> = match tokens.get(1) {
        Some(a) => match a {
            Token::Group(g) => {
                g.iter()
                    .map(|a| match a {
                        Token::String(s) => s.to_string(),
                        _ => "".to_string(),
                    })
                    .collect()
            }
            _ => return Err(gerr("Expected a group of column names (strings) at position 1")),
        },
        None => return Err(gerr("Missing column group (expected at position 1)")),
    };

    if let Some(tok) = tokens.get(2) {
        if match tok {
            Token::Keyword(a) if a.to_uppercase() == "ON" => false,
            _ => true,
        } {
            return Err(gerr(&format!(r#"In SEARCH command, expected keyword 'ON' at position 2, but found {:?}"#, tok)));
        }
    }

    if let Some(tok) = tokens.get(4) {
        if match tok {
            Token::Keyword(a) if a.to_uppercase() == "WHERE" => false,
            _ => true,
        } {
            return Err(gerr(r#"Expected keyword "WHERE" at position 4"#));
        }

        if let Some(iterator_of_tokens) = tokens.get(5..) {
            let mut bushes: Vec<Token> = Vec::new();

            for i in iterator_of_tokens {
                if bushes.len() == 3 {
                    conditions.0.push((
                        get_from_bushes_with_safety(&bushes, 0)?,
                        get_from_bushes_with_safety(&bushes, 1)?,
                        get_from_bushes_with_safety(&bushes, 2)?,
                    ));
                    bushes.clear();

                    conditions.1.push((
                        conditions.0.len() - 1,
                        match i {
                            Token::Keyword(a) => match a.to_uppercase().as_str() {
                                "OR" => 'o',
                                "AND" => 'a',
                                _ => {
                                    return Err(gerr(
                                        "Expected logical operator 'AND' or 'OR' after a condition",
                                    ))
                                }
                            },
                            _ => return Err(gerr("Expected logical operator after condition")),
                        },
                    ));
                    continue;
                }

                match i {
                    Token::String(_) => match bushes.len() {
                        0 | 2 => bushes.push(i.clone()),
                        _ => return Err(gerr("Unexpected string: operator might be missing")),
                    },
                    Token::Bool(_) | Token::Int(_) | Token::Float(_) => {
                        if bushes.len() == 2 {
                            bushes.push(i.clone())
                        } else {
                            return Err(gerr("Unexpected value: condition must follow 'column OP value' pattern"));
                        }
                    }
                    Token::Operator(_) => {
                        if bushes.len() == 1 {
                            bushes.push(i.clone());
                        } else {
                            return Err(gerr("Unexpected operator: check condition structure"));
                        }
                    }
                    _ => return Err(gerr("Unexpected token in WHERE clause")),
                }
            }

            if bushes.len() == 3 {
                conditions.0.push((
                    get_from_bushes_with_safety(&bushes, 0)?,
                    get_from_bushes_with_safety(&bushes, 1)?,
                    get_from_bushes_with_safety(&bushes, 2)?,
                ));
            }
        }
    }

    Ok(AST::Search(AstSearch {
        container,
        conditions,
        col_nam: columns,
    }))
}

fn debug_delete(tokens : &Vec<Token>) -> Result<AST,Error>{
    if let Some(t) = tokens.get(0){
        if let Token::Keyword(s) = t{
            if s.to_lowercase() != "delete".to_string(){
                return Err(gerr("Invalid keyword, expected \"DELETE\"."))
            }
        }else{
            return Err(gerr("Invalid token, expected Keyword(\"DELETE\")."))
        }
    }else{
        return Err(gerr("Missing tokens"))
    }
    let mut path = false;
    if let Some(t) = tokens.get(1){
        if let Token::Keyword(s) = t{
            match s.to_lowercase().as_str(){
                "row" => {path = true;},
                "container" => {path = false;},
                _ => {return Err(gerr("Invalid keyword, expected \"ROW\" or \"CONTAINER\""))}
            };
        }
    }else{
        return Err(gerr("Missing tokens"))
    }
    if !path{
        if let Some(t) = tokens.get(2){
            if let Token::String(s) = t{
                return Ok(AST::DeleteContainer(crate::AstDeleteContainer { container: s.to_string() }))
            }else{
                return Err(gerr("Missing container name"))
            }
        }else{
            return Err(gerr("Missing container name"))
        }
    }else{
        if let Some(t) = tokens.get(3){
            if let Token::Keyword(s) = t{
                if s.to_lowercase() != "on".to_string(){
                    return Err(gerr("Invalid keyword, expected \"ON\"."))
                }
            }else{
                return Err(gerr("Missing token, expected Keyword(\"ON\")."))
            }
        }else{
            return Err(gerr("Missing tokens"))
        }
        let mut container : String = String::new();
        let mut conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>) =
        (Vec::with_capacity(10), Vec::with_capacity(10));
        if let Some(t) = tokens.get(4){
            if let Token::String(s) = t{
                container = s.to_string();
            }else{
                drop(container);
                return Err(gerr("Missing container name"))
            }
        }else{
            return Err(gerr("Missing container name"))
        }

        if let Some(tok) = tokens.get(5) {
            if match tok {
                Token::Keyword(a) if a.to_uppercase() == "WHERE" => false,
                _ => true,
            } {
                return Err(gerr(r#"Expected keyword "WHERE" at position 5"#));
            }

            if let Some(iterator_of_tokens) = tokens.get(6..) {
                let mut bushes: Vec<Token> = Vec::new();

                for i in iterator_of_tokens {
                    if bushes.len() == 3 {
                        conditions.0.push((
                            get_from_bushes_with_safety(&bushes, 0)?,
                            get_from_bushes_with_safety(&bushes, 1)?,
                            get_from_bushes_with_safety(&bushes, 2)?,
                        ));
                        bushes.clear();

                        conditions.1.push((
                            conditions.0.len() - 1,
                            match i {
                                Token::Keyword(a) => match a.to_uppercase().as_str() {
                                    "OR" => 'o',
                                    "AND" => 'a',
                                    _ => {
                                        return Err(gerr(
                                            "Expected logical operator 'AND' or 'OR' after a condition",
                                        ))
                                    }
                                },
                                _ => return Err(gerr("Expected logical operator after condition")),
                            },
                        ));
                        continue;
                    }

                    match i {
                        Token::String(_) => match bushes.len() {
                            0 | 2 => bushes.push(i.clone()),
                            _ => return Err(gerr("Unexpected string: operator might be missing")),
                        },
                        Token::Bool(_) | Token::Int(_) | Token::Float(_) => {
                            if bushes.len() == 2 {
                                bushes.push(i.clone())
                            } else {
                                return Err(gerr("Unexpected value: condition must follow 'column OP value' pattern"));
                            }
                        }
                        Token::Operator(_) => {
                            if bushes.len() == 1 {
                                bushes.push(i.clone());
                            } else {
                                return Err(gerr("Unexpected operator: check condition structure"));
                            }
                        }
                        _ => return Err(gerr("Unexpected token in WHERE clause")),
                    }
                }

                if bushes.len() == 3 {
                    conditions.0.push((
                        get_from_bushes_with_safety(&bushes, 0)?,
                        get_from_bushes_with_safety(&bushes, 1)?,
                        get_from_bushes_with_safety(&bushes, 2)?,
                    ));
                }
            }
        }

        return Ok(AST::DeleteRow(crate::AstDeleteRow { container, conditions: Some(conditions) }))
    }
    
}

pub fn debug_tokens(tokens: &Vec<Token>) -> Result<AST, Error> {
    let first = tokens.first().ok_or_else(|| gerr("Token list is empty"))?;
    if let Token::Keyword(command) = first {
        return match command.to_uppercase().as_str() {
            "CREATE" => debug_create_command(tokens),
            "EDIT" => debug_edit_command(tokens),
            "SEARCH" => debug_search(tokens),
            "COMMIT"|"ROLLBACK" => debug_finishers_command(tokens),
            "DELETE" => debug_delete(tokens),
            _ => Err(gerr("Invalid command keyword")),
        }
    } else {
        Err(gerr("First token is not a keyword"))
    }
}


fn debug_finishers_command(tokens : &Vec<Token>) -> Result<AST,Error> {
    if let Some(kw) = tokens.get(0){
        if let Token::Keyword(st) = kw {
            match st.to_uppercase().as_str(){
                "COMMIT" => {
                    let con : Option<String> = match tokens.get(1){
                        Some(ttt) => match ttt{
                            Token::String(a) => Some(a.to_string()),
                            _ => None
                        },
                        None => None
                    };
                    return Ok(AST::Commit(AstCommit{
                        container:con
                    }))
                },
                "ROLLBACK" => {
                    let con : Option<String> = match tokens.get(1){
                        Some(ttt) => match ttt{
                            Token::String(a) => Some(a.to_string()),
                            _ => None
                        },
                        None => None
                    };
                    return Ok(AST::Rollback(AstRollback{
                        container:con
                    }))
                },
                _ => {
                    return Err(gerr("Failed to process the entered finisher"));
                }
            }
        }
    }
    return Err(gerr("Missing the instance to be editted"));
}


fn replace_arguments(token: Token, arg_iter: &mut impl Iterator<Item = Token>) -> Result<Token, Error> {
    match token {
        Token::Argument => {
            arg_iter.next().ok_or_else(|| gerr("Not enough arguments"))
        }
        Token::Group(tokens) => {
            let new_tokens = tokens.into_iter()
                .map(|t| replace_arguments(t, arg_iter))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Token::Group(new_tokens))
        }
        Token::SubCommand(tokens) => {
            let new_tokens = tokens.into_iter()
                .map(|t| replace_arguments(t, arg_iter))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Token::SubCommand(new_tokens))
        }
        other => Ok(other),
    }
}

// Replace arguments across the entire token list
fn replace_arguments_in_tokens(tokens: Vec<Token>, arguments: &[Token]) -> Result<Vec<Token>, Error> {
    let mut arg_iter = arguments.iter().cloned();
    let mut new_tokens = Vec::with_capacity(tokens.len());

    for token in tokens {
        new_tokens.push(replace_arguments(token, &mut arg_iter)?);
    }

    if arg_iter.next().is_some() {
        return Err(gerr("Too many arguments"));
    }

    Ok(new_tokens)
}


fn argument_lexer(input: String) -> Result<Vec<Token>, Error> {
    if input.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank"));
    }

    let mut result = Vec::with_capacity(1);

    if let Some(rest) = input.strip_prefix('§') {
        match B64ENGINE.decode(rest) {
            Ok(decoded) => result.push(Token::Bytes(decoded)),
            Err(e) => return Err(Error::new(ErrorKind::InvalidData, format!("Invalid base64: {}", e))),
        }
    } else {
        let lowercase = input.to_ascii_lowercase();

        if lowercase == "true" || lowercase == "false" {
            result.push(Token::Bool(lowercase == "true"));
        } else if let Ok(int_val) = input.parse::<i64>() {
            result.push(Token::Int(int_val));
        } else if let Ok(float_val) = input.parse::<f64>() {
            result.push(Token::Float(float_val));
        } else {
            result.push(Token::String(input));
        }
    }

    Ok(result)
}



pub fn parse(input: String, arguments_input: Vec<String>) -> Result<AST, Error> {
    let mut arguments: Vec<Token> = Vec::with_capacity(arguments_input.len());
    for arg_str in arguments_input {
        let toks = argument_lexer(arg_str)?;
        if toks.len() != 1 {
            println!("{:?}",toks);
            return Err(gerr("Each argument must lex into exactly one token"));
        }
        match &toks[0] {
            Token::Bool(a) => arguments.push(Token::Bool(*a)),
            Token::Int(a) => arguments.push(Token::Int(*a)),
            Token::Float(a) => arguments.push(Token::Float(*a)),
            Token::String(a) => arguments.push(Token::String(a.to_string())),
            Token::Keyword(s) => arguments.push(Token::String(s.to_string())),
            Token::Bytes(s) => arguments.push(Token::Bytes(s.to_owned())),
            _ => return Err(gerr("Invalid argument type; expected Bool, Int, Float, Bytes or String")),
        }
    }

    let tokens: Vec<Token> = lexer(input)?;

    let tokens_with_args = replace_arguments_in_tokens(tokens, &arguments)?;

    let a = debug_tokens(&tokens_with_args);
    println!("{:?}",a);
    a
}