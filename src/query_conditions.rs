use std::{collections::HashMap, io::{self, Error, ErrorKind}, mem::discriminant};
use regex::Regex;

use crate::container::get_index;
use crate::{alba_types::AlbaTypes, gerr, Token, query::PrimitiveQueryConditions, row::Row};


fn string_to_char(s: String) -> Result<char, io::Error> {
    let mut chars = s.chars();

    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(Error::new(ErrorKind::InvalidInput, "Input must be exactly one character")),
    }
}

#[derive(Clone, Copy, Debug)]
enum LogicalGate{
    And,
    Or,
}

#[derive(Clone,Debug)]
pub struct QueryConditionAtom{
    column : String,
    operator : Operator,
    value : AlbaTypes,
}
#[derive(Clone,Default,Debug)]
pub struct QueryConditions{
    primary_key : Option<String>,
    chain : Vec<(QueryConditionAtom,Option<LogicalGate>)>
}

#[derive(Debug)]
pub enum QueryIndexType {
    Strict(u64),
    // Range(Range<u64>),
    // InclusiveRange(RangeInclusive<u64>), 
}

#[derive(Debug)]
pub enum QueryType{
    Scan,
    Indexed(QueryIndexType),
}

#[derive(Clone,Debug)]
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

// ranges | infinity<bool> | InclusiveRange

/*  stuff I wanted to ask
- can cpu be processed in gpu even while with horrible performance?
*/
impl QueryConditions{
    pub fn from_primitive_conditions(primitive_conditions : PrimitiveQueryConditions, column_properties : &HashMap<String,AlbaTypes>,primary_key : String) -> Result<Self,Error>{
        let mut chain : Vec<(QueryConditionAtom,Option<LogicalGate>)> = Vec::new();
        let condition_chunk = primitive_conditions.0;
        let condition_logical_gates_vec = primitive_conditions.1;
        let mut condition_logical_gates = HashMap::new();
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
    pub fn row_match(&self, row: &Row,row_headers: &Vec<String>) -> Result<bool, Error> {
        
        
        if self.chain.is_empty() {
            
            return Ok(true);
        }
        
        let mut result = false;
        let mut regex_cache: HashMap<String, Regex> = HashMap::new();
        
        
        let len = self.chain.len();
        for (i,(query_condition, logical_gate)) in self.chain.iter().enumerate() {
            let column = &query_condition.column;
            let value = &query_condition.value;
            //println!("{:?}\t{:?}\t{:?}",query_condition,logical_gate,row);
            let ci = {
                let mut c = 0usize;
                for i in row.data.iter().zip(row_headers.iter()).enumerate(){
                    if *i.1.1 == *column{c = i.0;break;} ;
                }
                c
            };
            
            let row_value = if let Some(val) = row.data.get(ci) {
                
                val
            } else {
                
                continue;
            };
            
            let check = match query_condition.operator {
                Operator::Equal | Operator::StrictEqual => {
                    
                    
                    let result = *value == *row_value;
                    
                    result
                },
                Operator::Greater | Operator::GreaterEquality | Operator::Lower | Operator::LowerEquality => {
                    
                    
                    let opd = discriminant(&query_condition.operator);
                    let equality = (opd == discriminant(&Operator::GreaterEquality)) || 
                                  (opd == discriminant(&Operator::LowerEquality));
                    let lower = (opd == discriminant(&Operator::Lower)) || 
                               (opd == discriminant(&Operator::LowerEquality));
                    
                    
    
                    match (row_value, value) {
                        (AlbaTypes::Int(x), AlbaTypes::Int(y)) => {
                            
                            let result = if lower { if equality { x <= y } else { x < y } } 
                            else { if equality { x >= y } else { x > y } };
                            
                            result
                        },
                        (AlbaTypes::Bigint(x), AlbaTypes::Bigint(y)) => {
                            
                            let result = if lower { if equality { x <= y } else { x < y } } 
                            else { if equality { x >= y } else { x > y } };
                            result
                        },
                        (AlbaTypes::Float(x), AlbaTypes::Float(y)) => {
                            
                            let result = if lower { if equality { x <= y } else { x < y } } 
                            else { if equality { x >= y } else { x > y } };
                            
                            result
                        },
                        (AlbaTypes::Int(x), AlbaTypes::Bigint(y)) => {
                            let x_promoted = *x as i64;
                            
                            let result = if lower { if equality { x_promoted <= *y } else { x_promoted < *y } } 
                            else { if equality { x_promoted >= *y } else { x_promoted > *y } };
                            
                            result
                        },
                        (AlbaTypes::Bigint(x), AlbaTypes::Int(y)) => {
                            let y_promoted = *y as i64;
                            
                            let result = if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } };
                            
                            result
                        },
                        (AlbaTypes::Int(x), AlbaTypes::Float(y)) => {
                            let x_promoted = *x as f64;
                            
                            let result = if lower { if equality { x_promoted <= *y } else { x_promoted < *y } } 
                            else { if equality { x_promoted >= *y } else { x_promoted > *y } };
                            
                            result
                        },
                        (AlbaTypes::Float(x), AlbaTypes::Int(y)) => {
                            let y_promoted = *y as f64;
                            
                            let result = if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } };
                            
                            result
                        },
                        (AlbaTypes::Bigint(x), AlbaTypes::Float(y)) => {
                            let x_promoted = *x as f64;
                            
                            let result = if lower { if equality { x_promoted <= *y } else { x_promoted < *y } } 
                            else { if equality { x_promoted >= *y } else { x_promoted > *y } };
                            
                            result
                        },
                        (AlbaTypes::Float(x), AlbaTypes::Bigint(y)) => {
                            let y_promoted = *y as f64;
                            if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } }
                        },
                        _ => {
                            
                            return Err(gerr("Invalid type for numeric comparison"));
                        }
                    }
                },
                Operator::Different => {
                    *value != *row_value
                },
                Operator::StringContains | Operator::StringCaseInsensitiveContains => {
                    let case_insensitive = discriminant(&query_condition.operator) == 
                                          discriminant(&Operator::StringCaseInsensitiveContains);
                    
                    
                    
                    let row_string = match row_value {
                        AlbaTypes::Int(i) => i.to_string(),
                        AlbaTypes::Bigint(i) => i.to_string(),
                        AlbaTypes::Float(i) => i.to_string(),
                        AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | 
                        AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s.to_string(),
                        _ => {
                            
                            return Err(gerr("Invalid, the entered type cannot make string operations"));
                        }
                    };
                    
                    let value_string = match value {
                        AlbaTypes::Int(i) => i.to_string(),
                        AlbaTypes::Bigint(i) => i.to_string(),
                        AlbaTypes::Float(i) => i.to_string(),
                        AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | 
                        AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s.to_string(),
                        _ => {
                            
                            return Err(gerr("Invalid, the entered type cannot make string operations"));
                        }
                    };
    
                    if case_insensitive {
                        row_string.to_lowercase().contains(&value_string.to_lowercase())
                    } else {   
                        row_string.contains(&value_string)
                    }
                },
                Operator::StringRegularExpression => {
                    
                    
                    let row_string = match row_value {
                        AlbaTypes::Int(i) => i.to_string(),
                        AlbaTypes::Bigint(i) => i.to_string(),
                        AlbaTypes::Float(i) => i.to_string(),
                        AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | 
                        AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s.to_string(),
                        _ => {
                            
                            return Err(gerr("Invalid, the entered type cannot make string operations"));
                        }
                    };
                    
                    let value_string = match value {
                        AlbaTypes::Int(i) => i.to_string(),
                        AlbaTypes::Bigint(i) => i.to_string(),
                        AlbaTypes::Float(i) => i.to_string(),
                        AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | 
                        AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s.to_string(),
                        _ => {
                            
                            return Err(gerr("Invalid, the entered type cannot make string operations"));
                        }
                    };
    
                    
    
                    let regex_result = if let Some(cached_regex) = regex_cache.get(&value_string) {
                        cached_regex.is_match(&row_string)
                    } else {
                        
                        let re = Regex::new(&value_string);
                        match re {
                            Ok(compiled_regex) => {
                                let match_result = compiled_regex.is_match(&row_string);
                                
                                regex_cache.insert(value_string, compiled_regex);
                                match_result
                            },
                            Err(e) => {
                                
                                return Err(gerr(&e.to_string()));
                            }
                        }
                    };
                    
                    regex_result
                }
            };
            
            
            //println!("check:{}",check);
            if let Some(gate) = logical_gate {
                if i == len-1 {return Ok(check)}
                match gate {
                    LogicalGate::And => {
                        if !check {
                            result = false;
                            break;
                        }
                        
                    },
                    LogicalGate::Or => {
                        if check {
                            
                            result = true;
                            break;
                        }
                        
                    }
                }
            } else {
                result = check;
            }
        }
    
        
        Ok(result)
    }

    pub fn query_type(&self) -> Result<QueryType, Error> {
        if self.chain.is_empty() || self.primary_key.is_none() {
            return Ok(QueryType::Scan);
        }
        let pk = self.primary_key.clone().unwrap();
        let chain : Vec<(QueryConditionAtom,Option<LogicalGate>)> = self.chain.clone().into_iter().filter(|p|p.0.column == pk).collect();
        if chain.is_empty(){
            return Ok(QueryType::Scan)
        }
        for i in chain{
            match i.0.operator{
                Operator::Equal|Operator::StrictEqual => {
                    return Ok(QueryType::Indexed(QueryIndexType::Strict(get_index(i.0.value))))
                },
                _ => {continue;}
                
            }
        }
        Ok(QueryType::Scan)
    }

}
