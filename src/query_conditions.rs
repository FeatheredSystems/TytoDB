use std::{collections::HashMap, io::{self, Error, ErrorKind},mem::discriminant, ops::{Range, RangeInclusive}};

use ahash::AHashMap;
use regex::Regex;

use crate::{alba_types::AlbaTypes, gerr, indexing::GetIndex, lexer_functions::Token, query::PrimitiveQueryConditions, row::Row};


fn string_to_char(s: String) -> Result<char, io::Error> {
    let mut chars = s.chars();

    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(Error::new(ErrorKind::InvalidInput, "Input must be exactly one character")),
    }
}

#[derive(Clone, Copy)]
enum LogicalGate{
    And,
    Or,
}

#[derive(Clone)]
pub struct QueryConditionAtom{
    column : String,
    operator : Operator,
    value : AlbaTypes,
}
#[derive(Clone,Default)]
pub struct QueryConditions{
    primary_key : Option<String>,
    chain : Vec<(QueryConditionAtom,Option<LogicalGate>)>
}

fn gather_regex<'a>(regex_map: &'a mut HashMap<String, Regex>, key: String) -> Result<&'a Regex, Error> {
    if regex_map.contains_key(&key) {
        return Ok(regex_map.get(&key).unwrap());
    }
    let reg = match Regex::new(&key) {
        Ok(a) => a,
        Err(e) => return Err(gerr(&e.to_string())),
    };
    regex_map.insert(key.clone(), reg);
    Ok(regex_map.get(&key).unwrap())
}

pub enum QueryIndexType {
    Strict(u64),
    Range(Range<u64>),
    InclusiveRange(RangeInclusive<u64>), 
}

pub enum QueryType{
    Scan,
    Indexed(QueryIndexType),
}

#[derive(Clone)]
enum Operator{
    Equal,
    StrictEqual,
    Greater,
    Lower,
    GreaterEquality,
    LowerEquality,
    Different,
    StringContains,
    StringCaseInsensitiveContains,
    StringRegularExpression
}

impl QueryConditions{
    pub fn from_primitive_conditions(primitive_conditions : PrimitiveQueryConditions, column_properties : &HashMap<String,AlbaTypes>,primary_key : String) -> Result<Self,Error>{
        let mut chain : Vec<(QueryConditionAtom,Option<LogicalGate>)> = Vec::new();
        let condition_chunk = primitive_conditions.0;
        let condition_logical_gates_vec = primitive_conditions.1;
        let mut condition_logical_gates = AHashMap::new();
        for i in condition_logical_gates_vec{
            condition_logical_gates.insert(i.0, match i.1{
                'a'|'A' => LogicalGate::And,
                'o'|'O' => LogicalGate::Or,
                _ => return  Err(gerr("Failed to load LogicalGate, invalid token."))
            });
        }
        for (index,value) in condition_chunk.iter().enumerate(){
            let value = value.to_owned();
            
            let column = if let Token::String(name) = value.0{
                name
            }else{
                return Err(gerr("Failed to get QueryConditions, but failed to gather the column_name."))
            };
            
            let operator = if let Token::Operator(operator_name) = value.1{
                match operator_name.as_str(){
                    "=" => Operator::Equal,
                    "==" => Operator::StrictEqual,
                    ">=" => Operator::GreaterEquality,
                    "<=" => Operator::LowerEquality,
                    ">" => Operator::Greater,
                    "<" => Operator::Lower,
                    "!=" => Operator::Different,
                    "&>" => Operator::StringContains,
                    "&&>" => Operator::StringCaseInsensitiveContains,
                    "&&&>" => Operator::StringRegularExpression,
                    _ => {
                        return Err(gerr("Failed to get operator, invalid token contant."))
                    }
                }
            }else{
                return Err(gerr("Failed to get operator, invalid token,"))
            };

            let column_value = if let Some(column_type) = column_properties.get(&column){
                match column_type{
                    AlbaTypes::Text(_) => {
                        if let Token::String(string) = value.2{
                            AlbaTypes::Text(string)
                        }else {
                            return Err(gerr("No string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Int(_) => {
                        if let Token::Int(number) = value.2{
                            AlbaTypes::Int(number as i32)
                        }else {
                            return Err(gerr("No integer found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Bigint(_) => {
                        if let Token::Int(number) = value.2{
                            AlbaTypes::Bigint(number)
                        }else {
                            return Err(gerr("No integer found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Float(_) => {
                        if let Token::Float(number) = value.2{
                            AlbaTypes::Float(number)
                        }else {
                            return Err(gerr("No float found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Bool(_) => {
                        if let Token::Bool(bool) = value.2{
                            AlbaTypes::Bool(bool)
                        }else {
                            return Err(gerr("No bool found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Char(_) => {
                        if let Token::String(char) = value.2{
                            AlbaTypes::Char(string_to_char(char)?)
                        }else {
                            return Err(gerr("No char found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::NanoString(_) => {
                        if let Token::String(mut nano_string) = value.2{
                            nano_string.truncate(10);
                            AlbaTypes::NanoString(nano_string)
                        }else {
                            return Err(gerr("No nano_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::SmallString(_) => {
                        if let Token::String(mut small_string) = value.2{
                            small_string.truncate(100);
                            AlbaTypes::SmallString(small_string)
                        }else {
                            return Err(gerr("No small_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::MediumString(_) => {
                        if let Token::String(mut medium_string) = value.2{
                            medium_string.truncate(500);
                            AlbaTypes::SmallString(medium_string)
                        }else {
                            return Err(gerr("No medium_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::BigString(_) => {
                        if let Token::String(mut big_string) = value.2{
                            big_string.truncate(2000);
                            AlbaTypes::SmallString(big_string)
                        }else {
                            return Err(gerr("No big_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::LargeString(_) => {
                        if let Token::String(mut large_string) = value.2{
                            large_string.truncate(3000);
                            AlbaTypes::SmallString(large_string)
                        }else {
                            return Err(gerr("No large_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::NanoBytes(_) => {
                        if let Token::Bytes(mut nano_bytes) = value.2{
                            nano_bytes.truncate(10);
                            AlbaTypes::NanoBytes(nano_bytes)
                        }else {
                            return Err(gerr("No nano_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::SmallBytes(_) => {
                        if let Token::Bytes(mut small_bytes) = value.2{
                            small_bytes.truncate(1000);
                            AlbaTypes::SmallBytes(small_bytes)
                        }else {
                            return Err(gerr("No small_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::MediumBytes(_) => {
                        if let Token::Bytes(mut medium_bytes) = value.2{
                            medium_bytes.truncate(10000);
                            AlbaTypes::MediumBytes(medium_bytes)
                        }else {
                            return Err(gerr("No medium_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::BigSBytes(_) => {
                        if let Token::Bytes(mut big_bytes) = value.2{
                            big_bytes.truncate(100000);
                            AlbaTypes::BigSBytes(big_bytes)
                        }else {
                            return Err(gerr("No big_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::LargeBytes(_) => {
                        if let Token::Bytes(mut large_bytes) = value.2{
                            large_bytes.truncate(1000000);
                            AlbaTypes::BigSBytes(large_bytes)
                        }else {
                            return Err(gerr("No large_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::NONE => {
                        return Err(gerr("Failed to extract the value from the column_properties"))
                    },
                } 
            }else{
                return Err(gerr("Failed to generate QueryConditions, that happened because no column_property has been found with the given column-names"))
            };

            let gate = condition_logical_gates
                .get(&index)
                .map(|a| a.clone());

            chain.push((QueryConditionAtom{column,operator,value:column_value},gate));
        }
        return Ok(QueryConditions { chain, primary_key : Some(primary_key)})
    }
    pub fn row_match(&self,row : &Row) -> Result<bool,Error>{
        if self.chain.is_empty(){
            return Ok(true)
        }
        let mut regex_cache : HashMap<String,Regex> = HashMap::new();
        let mut result = false;
        for (query_atom,logicgate) in self.chain.iter(){
            let value = if let Some(val) = row.data.get(&query_atom.column){
                val
            }else{
                return Err(gerr("No value found to that column name"))
            };
            let check = match query_atom.operator{
                Operator::Equal|Operator::StrictEqual => {
                    query_atom.value == *value
                },
                Operator::Greater => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n > match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n > match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n > match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::Lower => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n < match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n < match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n < match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::GreaterEquality => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n >= match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n >= match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n >= match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::LowerEquality => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n <= match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n <= match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n <= match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::Different => {
                    query_atom.value != *value
                },
                Operator::StringContains => {
                    match &query_atom.value{
                        AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)|AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => {
                            let val = match value{
                                AlbaTypes::NanoString(s) => s,
                                AlbaTypes::SmallString(s) => s,
                                AlbaTypes::MediumString(s) => s,
                                AlbaTypes::BigString(s) => s,
                                AlbaTypes::LargeString(s) => s,
                                AlbaTypes::Text(s) => s,
                                _ => return Err(gerr("Invalid row type"))
                            };
                            s.contains(val)
                        },
                        _ => {
                            return Err(gerr("Invalid row type"))
                        }
                    }
                },
                Operator::StringCaseInsensitiveContains => {
                    match &query_atom.value{
                        AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)|AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => {
                            let val = match value{
                                AlbaTypes::NanoString(s) => s,
                                AlbaTypes::SmallString(s) => s,
                                AlbaTypes::MediumString(s) => s,
                                AlbaTypes::BigString(s) => s,
                                AlbaTypes::LargeString(s) => s,
                                AlbaTypes::Text(s) => s,
                                _ => return Err(gerr("Invalid row type"))
                            };
                            s.to_lowercase().contains(&val.to_lowercase())
                        },
                        _ => {
                            return Err(gerr("Invalid row type"))
                        }
                    }
                },
                Operator::StringRegularExpression => {
                    match &query_atom.value{
                        AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)|AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => {
                            let val = match value{
                                AlbaTypes::NanoString(s) => s,
                                AlbaTypes::SmallString(s) => s,
                                AlbaTypes::MediumString(s) => s,
                                AlbaTypes::BigString(s) => s,
                                AlbaTypes::LargeString(s) => s,
                                AlbaTypes::Text(s) => s,
                                _ => return Err(gerr("Invalid row type"))
                            };
                            let reg = gather_regex(&mut regex_cache, val.clone())?;
                            reg.is_match(&s)
                        },
                        _ => {
                            return Err(gerr("Invalid row type"))
                        }
                    }
                },
            };
            result = check;
            if let Some(logical_gate) = logicgate{
                match logical_gate {
                    LogicalGate::And => if !check{return Ok(false)},
                    LogicalGate::Or => if check{return Ok(true)} 
                }
            }
        }

        Ok(result)
    }
    pub fn raw_chain(&self) -> Vec<QueryConditionAtom>{
        return self.chain.iter().cloned().map(|group|group.0).collect()
    }
    pub fn query_type(&self) -> Result<QueryType,Error>{
        let mut raw_chain = self.raw_chain();
        if raw_chain.is_empty(){
            return Ok(QueryType::Scan) 
        }
        let primary_key = if let Some(pk) = &self.primary_key{
            pk
        }else{
            return Ok(QueryType::Scan)
        };
        let mut sorting = (
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
            Vec::with_capacity(raw_chain.len()/10),
        );
    
        for query_atom in raw_chain {
            match query_atom.operator {
                Operator::Equal => sorting.0.push(query_atom),
                Operator::StrictEqual => sorting.1.push(query_atom),
                Operator::Greater => sorting.4.push(query_atom),
                Operator::Lower => sorting.5.push(query_atom),
                Operator::GreaterEquality => sorting.2.push(query_atom),
                Operator::LowerEquality => sorting.3.push(query_atom),
                Operator::Different => sorting.6.push(query_atom),
                Operator::StringContains => sorting.7.push(query_atom),
                Operator::StringCaseInsensitiveContains => sorting.8.push(query_atom),
                Operator::StringRegularExpression => sorting.9.push(query_atom),
            }
        }
        raw_chain = Vec::new();

        for vec in [&sorting.0, &sorting.1, &sorting.2, &sorting.3, &sorting.4, &sorting.5, &sorting.6, &sorting.7, &sorting.8, &sorting.9] {
            let secvec = vec;
            for i in secvec{
                raw_chain.push((*i).clone());
            }
        }
        drop(sorting);
        if raw_chain.len() == 0{
            return Ok(QueryType::Scan);
        }       
        let first : &QueryConditionAtom = &raw_chain[0];
        let is_equality : bool = match first.operator{
            Operator::Equal | Operator::StrictEqual => true,
            _ => false
        };
        if is_equality && first.column == *primary_key{
            return Ok(QueryType::Indexed(QueryIndexType::Strict(first.value.get_index())))
        }
        // 0 = Scan
        // 1 = RangeEquality
        // 2 = Range
        let mut typo_range : (u64,u64)= (0,0);
        let typo : u8 = match first.operator{
            Operator::GreaterEquality => {
                let mut a = 0;
                for i in raw_chain.iter(){
                    if discriminant(&i.operator) == discriminant(&Operator::LowerEquality){
                        typo_range = (first.value.get_index(),i.value.get_index());
                        a=1;
                    }
                    if discriminant(&i.operator) == discriminant(&Operator::Lower){
                        typo_range = (first.value.get_index(),i.value.get_index());
                        a=2;
                    }
                }
                a
            },
            Operator::Greater => {
                let mut a = 0;
                for i in raw_chain.iter(){
                    if discriminant(&i.operator) == discriminant(&Operator::LowerEquality){
                        typo_range = (first.value.get_index(),i.value.get_index());
                        a=1
                    }
                    if discriminant(&i.operator) == discriminant(&Operator::Lower){
                        typo_range = (first.value.get_index(),i.value.get_index());
                        a=2
                    }
                }
                a
            },
            _ => {0}
        };
        return Ok(match typo{
            0 => QueryType::Scan,
            1 => QueryType::Indexed(QueryIndexType::Range(typo_range.0..typo_range.1)),
            2 => QueryType::Indexed(QueryIndexType::InclusiveRange(typo_range.0..=typo_range.1)),
            _ => QueryType::Scan,
        })
    }
}