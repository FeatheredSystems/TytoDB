use std::{collections::HashMap, io::{self, Error, ErrorKind}, mem::discriminant, ops::{Range, RangeInclusive}};

use aes_gcm::aead::consts::U9223372036854775808;
use ahash::AHashMap;
use regex::{Regex, Replacer};

use crate::{alba_types::AlbaTypes, gerr, indexing::GetIndex, lexer_functions::Token, loginfo, query::PrimitiveQueryConditions, row::Row};


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

#[derive(Debug)]
pub enum QueryIndexType {
    Strict(u64),
    Range(Range<u64>),
    InclusiveRange(RangeInclusive<u64>), 
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
impl Operator{
    fn get_range(&self,ind : u64) -> LogicCell{
        match self{
            Operator::Equal | Operator::StrictEqual => {((ind,ind),(false,false),true)},
            Operator::Greater => {((ind,0),(false,true),false)},
            Operator::Lower => {((0,ind),(true,false),false)},
            Operator::GreaterEquality => {((ind,0),(false,true),true)},
            Operator::LowerEquality => {((0,ind),(true,false),true)},
            Operator::Different => {((0,0),(false,false),false)},
            Operator::StringContains => {((0,0),(false,false),false)},
            Operator::StringCaseInsensitiveContains => {((0,0),(false,false),false)},
            Operator::StringRegularExpression => {((0,0),(false,false),false)},
        }
    }
}

type LogicCell = ((u64,u64),(bool,bool),bool);
// ranges | infinity<bool> | InclusiveRange

/*  stuff I wanted to ask
- can cpu be processed in gpu even while with horrible performance?
*/
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
    pub fn row_match(&self, row: &Row) -> Result<bool, Error> {
        
        
        if self.chain.is_empty() {
            
            return Ok(true);
        }
        
        let mut result = false;
        let mut regex_cache: AHashMap<String, Regex> = AHashMap::new();
        
        
    
        for (index, (query_condition, logical_gate)) in self.chain.iter().enumerate() {
            let column = &query_condition.column;
            let value = &query_condition.value;
            
            
            
            let row_value = if let Some(val) = row.data.get(column) {
                
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
                    
                    
    
                    let comparison_result = match (row_value, value) {
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
                            
                            let result = if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } };
                            
                            result
                        },
                        _ => {
                            
                            return Err(gerr("Invalid type for numeric comparison"));
                        }
                    };
                    
                    comparison_result
                },
                Operator::Different => {
                    
                    
                    let result = *value != *row_value;
                    
                    result
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
    
                    let result = if case_insensitive {
                        
                        row_string.to_lowercase().contains(&value_string.to_lowercase())
                    } else {
                        
                        row_string.contains(&value_string)
                    };
                    
                    
                    result
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
                        
                        let match_result = cached_regex.is_match(&row_string);
                        
                        match_result
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
            
            
            
            if let Some(gate) = logical_gate {
                
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
        loginfo!("Starting query type analysis");
        
        // Early return for scan conditions
        if self.chain.is_empty() || self.primary_key.is_none() {
            loginfo!("No chain or primary key found, defaulting to Scan");
            return Ok(QueryType::Scan);
        }
        
        let pk = self.primary_key.as_ref().unwrap();
        loginfo!("Primary key: {:?}, chain length: {}", pk, self.chain.len());
        
        let mut range: LogicCell = ((0, 0), (false, false), false);
        let l = self.chain.len();
        let mut logic_cells: Vec<LogicCell> = Vec::with_capacity(l);
        let mut gates: Vec<bool> = Vec::with_capacity(l);
        let mut range_mutated = false;
        
        let ad = discriminant(&LogicalGate::And);
        
        // Process chain into logic cells and gates
        loginfo!("Processing {} chain elements", self.chain.len());
        for (idx, i) in self.chain.iter().enumerate() {
            if i.0.column != *pk {
                loginfo!("Chain[{}]: Column {:?} != primary key, skipping", idx, i.0.column);
                logic_cells.push(((0, 0), (false, false), false));
            } else {
                let index = i.0.value.get_index();
                let cell = i.0.operator.get_range(index);
                loginfo!("Chain[{}]: Primary key match, operator range: {:?}", idx, cell);
                if !range_mutated{
                    range = cell.clone();
                    range_mutated = true;
                }
                logic_cells.push(cell);
            }
            
            if let Some(logic_gate) = i.1 {
                let is_and = ad == discriminant(&logic_gate);
                loginfo!("Chain[{}]: Logic gate: {:?} (is_and: {})", idx, logic_gate, is_and);
                gates.push(is_and);
            }
        }

        loginfo!("Combining logic cells with gates");
        for (idx, (cell, gate)) in logic_cells.iter().zip(gates.iter()).enumerate() {
            loginfo!("Processing cell[{}]: {:?} with gate: {}", idx, cell, gate);
            
            if range == ((0, 0), (false, false), false) {
                loginfo!("First valid range, setting initial: {:?}", cell);
                range = *cell;
                continue;
            }
            
            if *gate { // AND operation
                loginfo!("AND operation - intersecting ranges");
                
                // Handle null flag
                if range.2 != cell.2 && cell.2 {
                    loginfo!("Setting null flag from cell");
                    range.2 = cell.2;
                }
                
                // Intersect upper bound
                if range.0.1 == 0 && range.1.1 == true && cell.0.1 != 0 && cell.1.1 == false {
                    loginfo!("Updating upper bound from cell: {} -> {}", range.0.1, cell.0.1);
                    range.0.1 = cell.0.1;
                    range.1.1 = false;
                }
                
                // Intersect lower bound
                if range.0.0 == 0 && range.1.0 == true && cell.0.0 != 0 && cell.1.0 == false {
                    loginfo!("Updating lower bound from cell: {} -> {}", range.0.0, cell.0.0);
                    range.0.0 = cell.0.0;
                    range.1.0 = false;
                }
            } else { // OR operation
                loginfo!("OR operation - union of ranges");
                
                // Handle null flag
                if range.2 || cell.2 {
                    loginfo!("Setting null flag due to OR");
                    range.2 = true;
                }
                
                // Expand lower bound (take minimum)
                if cell.0.0 != 0 && (range.0.0 == 0 || cell.0.0 < range.0.0) {
                    loginfo!("Expanding lower bound: {} -> {}", range.0.0, cell.0.0);
                    range.0.0 = cell.0.0;
                    range.1.0 = cell.1.0;
                }
                
                // Expand upper bound (take maximum)
                if cell.0.1 != 0 && (range.0.1 == 0 || cell.0.1 > range.0.1) {
                    loginfo!("Expanding upper bound: {} -> {}", range.0.1, cell.0.1);
                    range.0.1 = cell.0.1;
                    range.1.1 = cell.1.1;
                }
            }
            
            loginfo!("Range after processing cell[{}]: {:?}", idx, range);
        }
        
        // Normalize range bounds
        if range.0.0 == 0 && range.1.0 {
            loginfo!("Normalizing lower bound to 0 (inclusive)");
            range.0.0 = 0;
            range.1.0 = false;
        }
        if range.0.1 == 0 && range.1.1 {
            loginfo!("Normalizing upper bound to MAX (exclusive)");
            range.0.1 = u64::MAX;
            range.1.1 = false;
        }
        
        loginfo!("Final range: {:?}", range);
        
        // Determine query type based on final range
        if range == ((0, 0), (false, false), false) {
            loginfo!("Empty range, returning Scan");
            return Ok(QueryType::Scan);
        }
        
        if range.0.0 > range.0.1 {
            loginfo!("Invalid range (lower > upper), returning Scan");
            return Ok(QueryType::Scan);
        }
        
        if range.2 && range.0.0 == range.0.1 {
            loginfo!("Strict index query for value: {}", range.0.0);
            return Ok(QueryType::Indexed(QueryIndexType::Strict(range.0.0)));
        }
        
        if range.2 && range.0.0 != range.0.1 {
            loginfo!("Inclusive range query: {}..={}", range.0.0, range.0.1);
            return Ok(QueryType::Indexed(QueryIndexType::InclusiveRange(range.0.0..=range.0.1)));
        }
        
        if !range.2 && range.0.0 != range.0.1 {
            loginfo!("Exclusive range query: {}..{}", range.0.0, range.0.1);
            return Ok(QueryType::Indexed(QueryIndexType::Range(range.0.0..range.0.1)));
        }
        
        loginfo!("Fallback to Scan query type");
        Ok(QueryType::Scan)
    }
}