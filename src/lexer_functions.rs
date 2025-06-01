use lazy_static::lazy_static;
use std::io::Error;
use base64::{alphabet, engine::{self, general_purpose}, Engine as _};

use crate::lexer;

#[derive(Debug, Clone, PartialEq)]
pub enum Token{
    Keyword(String),
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Float(f64),
    Bool(bool),
    Operator(String),
    Group(Vec<Token>),
    SubCommand(Vec<Token>),
    Argument,
}

const KEYWORDS: &[&str] = &[
    "CREATE",
    "COMMIT",
    "ROLLBACK",
    "DELETE",
    "EDIT",
    "SEARCH",
    "WHERE",
    "ROW",
    "CONTAINER",
    "ON",
    "USING",
    "INT",
    "BIGINT",
    "TEXT",
    "BOOL",
    "FLOAT",
    "AND",
    "OR",
    "NANO-STRING",
    "SMALL-STRING",
    "MEDIUM-STRING",
    "BIG-STRING",
    "LARGE-STRING",
    "NANO-BYTES",
    "SMALL-BYTES",
    "MEDIUM-BYTES",
    "BIG-BYTES",
    "LARGE-BYTES",

    // weird looking because connection handlers that should use this, not users
    "QYCNPVS", // query control previous 
    "QYCNNXT", // query control next
    "QYCNEXT" // query control exit
];

pub fn lexer_keyword_match(result: &mut Vec<Token>, dough: &mut String) -> bool {
    let keyword = dough.to_uppercase(); 

    if KEYWORDS.contains(&keyword.as_str()) {
        result.push(Token::Keyword(keyword.to_uppercase())); 
        dough.clear(); 
        return true
    }
    false
}

pub fn lexer_string_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut T) -> bool{
    if dough.starts_with(' '){
        dough.drain(..1);
    }
    if dough.starts_with('\'') || dough.starts_with('"'){
        if let Some(quote_type) = dough.chars().nth(0){
            let mut escaped = false;
            while let Some(s) = itr.next() {
                if s == '\\'{
                    escaped = true
                }
                dough.push(s);
                
                if s == quote_type && !escaped {
                    break;
                }
            }
            if dough.starts_with(quote_type) && dough.ends_with(quote_type){
                result.push(Token::String(dough[1..dough.len()-1].to_string()));
                dough.clear();
                return true
            }
        }
    }
    false
}    

fn split_group_args(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::with_capacity(input.len());
    let (mut in_string, mut string_sort, mut parens, mut escape) = (false, '\0', 0, false);

    for c in input.chars() {
        if escape {
            current.push(c);
            escape = false;
            continue;
        }
        match c {
            '\\' => { escape = true; current.push(c); }
            '\'' | '"' => {
                if !in_string { in_string = true; string_sort = c; }
                else if c == string_sort { in_string = false; }
                current.push(c);
            }
            '(' if !in_string => { parens += 1; current.push(c); }
            ')' if !in_string => { if parens > 0 { parens -= 1; } current.push(c); }
            ',' if !in_string && parens == 0 => {
                let t = current.trim();
                if !t.is_empty() { result.push(t.to_string()); }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    let t = current.trim();
    if !t.is_empty() { result.push(t.to_string()); }

    result
}


pub fn lexer_group_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut T,
) -> bool {
    if dough.starts_with('[') {
        let mut in_string : bool = false;
        let mut string_sort : char = '\\';
        let mut i = 1;
        while let Some(c) = itr.next() {
            dough.push(c);
            if (c == '\'' || c == '"') && !in_string{
                string_sort = c;
                in_string = true;
                continue;
            }
            if in_string && c == string_sort{
                in_string = false;
                continue;
            }
            if c == '[' && !in_string{
                i+=1;
            }
            if c == ']' && !in_string {
                i -= 1;
                if i == 0{
                    break;
                }
            }
        }
        ////println!("{}",dough);

        if dough.ends_with(']') {
            let inner = &dough[1..dough.len() - 1];
            let mut abstract_tokens = Vec::with_capacity(16);

            for part in split_group_args(&inner) {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                match lexer(part.to_string()) {
                    Ok(mut toks) if !toks.is_empty() => {
                        abstract_tokens.push(toks.remove(0));
                    }
                    _ => {
                        continue;
                    }
                }
            }

            dough.clear();
            result.push(Token::Group(abstract_tokens));
            return true;
        }
    }

    false
}
pub fn lexer_subcommand_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut T,
) -> Result<bool, Error> {
    if dough.starts_with('(') {
        let mut in_string : bool = false;
        let mut string_sort : char = '\\';
        while let Some(c) = itr.next() {
            dough.push(c);
            if (c == '\'' || c == '"') && !in_string{
                string_sort = c;
                in_string = true;
                continue;
            }
            if in_string && c == string_sort{
                in_string = false;
                continue;
            }
            if c == ')' && !in_string {
                break;
            }
        }
        ////println!("{}",dough);

        if dough.ends_with(')') {
            let inner = &dough.clone()[1..dough.len() - 1];
            dough.clear();
            result.push(Token::SubCommand(lexer(inner.to_string())?));
            return Ok(true);
        }
    }

    Ok(false)
}

const RADIX : u32= 10;
pub fn lexer_number_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut std::iter::Peekable<T>) -> bool{
    if let Some(d) = dough.chars().nth(0){
        let mut had_dot = false;
        let mut negative = false;
        if d.is_digit(RADIX) || d == '-'{
            if d == '-'{
                negative = true;
            }
            let mut cn : u8 = 0;
            while let Some(n) = itr.next(){
                if n.is_digit(RADIX){
                    dough.push(n.clone());
                }else{
                    if n == '.' && !had_dot{
                        dough.push(n.clone());
                        had_dot = true;
                        continue;
                    }
                    if n == 'e' && cn == 0{
                        cn = 1;
                        dough.push(n.clone());
                        continue
                    }
                    if (n == '-' || n == '+') && cn == 1{
                        cn = 2;
                        dough.push(n.clone());
                        continue;
                    }
                    break;
                }
            }
            if cn == 2 && (dough.ends_with("e+")||dough.ends_with("e-")){
                return false
            }
            if had_dot{
                if let Ok(float) = dough.parse::<f64>(){
                    result.push(Token::Float(if negative {float*-1.0}else{float}));
                    dough.clear();
                    return true
                }
            }else{
                if let Ok(int) = dough.parse::<i64>(){
                    result.push(Token::Int(if negative {int*-1}else{int}));
                    dough.clear();
                    return true
                }
            }
        }
    }
    false
}  
pub fn lexer_ignore_comments_match<T:Iterator<Item = char>>(dough : &mut String, itr : &mut std::iter::Peekable<T>) -> bool{
    if dough.starts_with('/'){
        while let Some(c) = itr.next(){
            dough.push(c);
            let n = dough.len();
            if &dough[n-2..n] == "*/"{
                break;
            }
        }
    }
    false
}
pub fn lexer_operator_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut std::iter::Peekable<T>
) -> bool {
    if dough.is_empty() {
        return false;
    }
    let first = dough.chars().next().unwrap();
    let op_abstract_token = match first {
        '>' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    ">="
                } else {
                    ">"
                }
            } else {
                ">"
            }
        },
        '<' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    "<="
                } else {
                    "<"
                }
            } else {
                "<"
            }
        },
        '&' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '&' {
                    itr.next();
                    if let Some(&third_char) = itr.peek() {
                        if third_char == '&' {
                            itr.next();
                            if let Some(&fourth_char) = itr.peek() {
                                if fourth_char == '>' {
                                    itr.next();
                                    "&&&>"
                                } else {
                                    "&&&"
                                }
                            } else {
                                "&&&"
                            }
                        } else if third_char == '>' {
                            itr.next();
                            "&&>"
                        } else {
                            "&&"
                        }
                    } else {
                        "&&"
                    }
                } else if next_char == '>' {
                    itr.next();
                    "&>"
                } else {
                    "&"
                }
            } else {
                "&"
            }
        },
        '=' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    "=="
                } else {
                    "=" 
                }
            } else {
                "="
            }
        },
        '+' => "+",
        '*' => "*",
        '-' => "-",
        '/' => "/",
        '%' => "%",
        '!' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    "!="
                } else {
                    "!" 
                }
            } else {
                "!"
            }
        },
        _ => return false, 
    };

    result.push(Token::Operator(op_abstract_token.to_string()));
    dough.clear();
    true
}

pub fn lexer_boolean_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    _itr: &mut std::iter::Peekable<T>
) -> bool{
    let trimmed = dough.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        result.push(Token::Bool(true));
        dough.clear();
        return true
    } else if trimmed.eq_ignore_ascii_case("false") {
        result.push(Token::Bool(false));
        dough.clear();
        return true
    }
    false
}


fn geteng() -> general_purpose::GeneralPurpose{
    let crazy_config = engine::GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_encode_padding(true)
        .with_decode_padding_mode(engine::DecodePaddingMode::Indifferent);
    let eng: general_purpose::GeneralPurpose = base64::engine::GeneralPurpose::new(&alphabet::Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/").unwrap(), crazy_config);
    eng
}

lazy_static!{
    pub static ref B64ENGINE : general_purpose::GeneralPurpose = geteng();
}

pub fn lexer_bytes_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    _itr: &mut std::iter::Peekable<T>
) -> bool{
    // let trimmed = dough.trim();
    // if trimmed.eq_ignore_ascii_case("true") {
    //     result.push(Token::Bool(true));
    //     dough.clear();
    //     return true
    // } else if trimmed.eq_ignore_ascii_case("false") {
    //     result.push(Token::Bool(false));
    //     dough.clear();
    //     return true
    // }
    // false
    if !dough.starts_with("§"){
        return false
    }
    if let Some(rest) = dough.strip_prefix("§") {
        if let Ok(b) = B64ENGINE.decode(rest) {
            result.push(Token::Bytes(b));
            dough.clear();
            return true;
        }
    }
    false
}

