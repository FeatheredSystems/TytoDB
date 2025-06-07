use std::{collections::HashMap, io::{self, Error, ErrorKind}, mem::discriminant, ops::{Range, RangeInclusive}};

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
        loginfo!("Starting row_match evaluation");
        
        if self.chain.is_empty() {
            loginfo!("Query chain is empty, returning true (matches all)");
            return Ok(true);
        }
        
        let mut result = false;
        let mut regex_cache: AHashMap<String, Regex> = AHashMap::new();
        
        loginfo!("Processing {} conditions in query chain", self.chain.len());
    
        for (index, (query_condition, logical_gate)) in self.chain.iter().enumerate() {
            let column = &query_condition.column;
            let value = &query_condition.value;
            
            loginfo!("Evaluating condition {} for column '{}'", index + 1, column);
            
            let row_value = if let Some(val) = row.data.get(column) {
                loginfo!("Found column '{}' in row data", column);
                val
            } else {
                loginfo!("Column '{}' not found in row, skipping condition", column);
                continue;
            };
            
            let check = match query_condition.operator {
                Operator::Equal | Operator::StrictEqual => {
                    loginfo!("Performing equality check for column '{}'", column);
                    loginfo!("Comparing values - Row value: {:?}, Query value: {:?}", row_value, value);
                    let result = *value == *row_value;
                    loginfo!("Equality check result: {}", result);
                    result
                },
                Operator::Greater | Operator::GreaterEquality | Operator::Lower | Operator::LowerEquality => {
                    loginfo!("Performing numeric comparison for column '{}'", column);
                    
                    let opd = discriminant(&query_condition.operator);
                    let equality = (opd == discriminant(&Operator::GreaterEquality)) || 
                                  (opd == discriminant(&Operator::LowerEquality));
                    let lower = (opd == discriminant(&Operator::Lower)) || 
                               (opd == discriminant(&Operator::LowerEquality));
                    
                    loginfo!("Comparison type: {} (equality: {}, lower: {})", 
                            match query_condition.operator {
                                Operator::Greater => "Greater",
                                Operator::GreaterEquality => "GreaterEquality", 
                                Operator::Lower => "Lower",
                                Operator::LowerEquality => "LowerEquality",
                                _ => "Unknown"
                            }, equality, lower);
    
                    let comparison_result = match (row_value, value) {
                        (AlbaTypes::Int(x), AlbaTypes::Int(y)) => {
                            loginfo!("Comparing Int({}) with Int({})", x, y);
                            let result = if lower { if equality { x <= y } else { x < y } } 
                            else { if equality { x >= y } else { x > y } };
                            loginfo!("Int comparison result: {} {} {} = {}", x, 
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y, result);
                            result
                        },
                        (AlbaTypes::Bigint(x), AlbaTypes::Bigint(y)) => {
                            loginfo!("Comparing Bigint({}) with Bigint({})", x, y);
                            let result = if lower { if equality { x <= y } else { x < y } } 
                            else { if equality { x >= y } else { x > y } };
                            loginfo!("Bigint comparison result: {} {} {} = {}", x,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y, result);
                            result
                        },
                        (AlbaTypes::Float(x), AlbaTypes::Float(y)) => {
                            loginfo!("Comparing Float({}) with Float({})", x, y);
                            let result = if lower { if equality { x <= y } else { x < y } } 
                            else { if equality { x >= y } else { x > y } };
                            loginfo!("Float comparison result: {} {} {} = {}", x,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y, result);
                            result
                        },
                        (AlbaTypes::Int(x), AlbaTypes::Bigint(y)) => {
                            let x_promoted = *x as i64;
                            loginfo!("Comparing Int({}) promoted to i64({}) with Bigint({})", x, x_promoted, y);
                            let result = if lower { if equality { x_promoted <= *y } else { x_promoted < *y } } 
                            else { if equality { x_promoted >= *y } else { x_promoted > *y } };
                            loginfo!("Int->i64 vs Bigint comparison result: {} {} {} = {}", x_promoted,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y, result);
                            result
                        },
                        (AlbaTypes::Bigint(x), AlbaTypes::Int(y)) => {
                            let y_promoted = *y as i64;
                            loginfo!("Comparing Bigint({}) with Int({}) promoted to i64({})", x, y, y_promoted);
                            let result = if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } };
                            loginfo!("Bigint vs Int->i64 comparison result: {} {} {} = {}", x,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y_promoted, result);
                            result
                        },
                        (AlbaTypes::Int(x), AlbaTypes::Float(y)) => {
                            let x_promoted = *x as f64;
                            loginfo!("Comparing Int({}) promoted to f64({}) with Float({})", x, x_promoted, y);
                            let result = if lower { if equality { x_promoted <= *y } else { x_promoted < *y } } 
                            else { if equality { x_promoted >= *y } else { x_promoted > *y } };
                            loginfo!("Int->f64 vs Float comparison result: {} {} {} = {}", x_promoted,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y, result);
                            result
                        },
                        (AlbaTypes::Float(x), AlbaTypes::Int(y)) => {
                            let y_promoted = *y as f64;
                            loginfo!("Comparing Float({}) with Int({}) promoted to f64({})", x, y, y_promoted);
                            let result = if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } };
                            loginfo!("Float vs Int->f64 comparison result: {} {} {} = {}", x,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y_promoted, result);
                            result
                        },
                        (AlbaTypes::Bigint(x), AlbaTypes::Float(y)) => {
                            let x_promoted = *x as f64;
                            loginfo!("Comparing Bigint({}) promoted to f64({}) with Float({})", x, x_promoted, y);
                            let result = if lower { if equality { x_promoted <= *y } else { x_promoted < *y } } 
                            else { if equality { x_promoted >= *y } else { x_promoted > *y } };
                            loginfo!("Bigint->f64 vs Float comparison result: {} {} {} = {}", x_promoted,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y, result);
                            result
                        },
                        (AlbaTypes::Float(x), AlbaTypes::Bigint(y)) => {
                            let y_promoted = *y as f64;
                            loginfo!("Comparing Float({}) with Bigint({}) promoted to f64({})", x, y, y_promoted);
                            let result = if lower { if equality { *x <= y_promoted } else { *x < y_promoted } } 
                            else { if equality { *x >= y_promoted } else { *x > y_promoted } };
                            loginfo!("Float vs Bigint->f64 comparison result: {} {} {} = {}", x,
                                    match query_condition.operator {
                                        Operator::Greater => ">",
                                        Operator::GreaterEquality => ">=",
                                        Operator::Lower => "<",
                                        Operator::LowerEquality => "<=",
                                        _ => "?"
                                    }, y_promoted, result);
                            result
                        },
                        _ => {
                            loginfo!("Invalid type combination for numeric comparison - Row: {:?}, Query: {:?}", row_value, value);
                            return Err(gerr("Invalid type for numeric comparison"));
                        }
                    };
                    
                    comparison_result
                },
                Operator::Different => {
                    loginfo!("Performing inequality check for column '{}'", column);
                    loginfo!("Comparing values for inequality - Row value: {:?}, Query value: {:?}", row_value, value);
                    let result = *value != *row_value;
                    loginfo!("Inequality check result: {}", result);
                    result
                },
                Operator::StringContains | Operator::StringCaseInsensitiveContains => {
                    let case_insensitive = discriminant(&query_condition.operator) == 
                                          discriminant(&Operator::StringCaseInsensitiveContains);
                    
                    loginfo!("Performing string contains check (case_insensitive: {}) for column '{}'", 
                            case_insensitive, column);
                    
                    let row_string = match row_value {
                        AlbaTypes::Int(i) => i.to_string(),
                        AlbaTypes::Bigint(i) => i.to_string(),
                        AlbaTypes::Float(i) => i.to_string(),
                        AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | 
                        AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s.to_string(),
                        _ => {
                            loginfo!("Invalid type for string operations - Row value: {:?}", row_value);
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
                            loginfo!("Invalid type for string operations - Query value: {:?}", value);
                            return Err(gerr("Invalid, the entered type cannot make string operations"));
                        }
                    };
    
                    let result = if case_insensitive {
                        loginfo!("Checking if '{}' contains '{}' (case insensitive)", 
                                row_string, value_string);
                        row_string.to_lowercase().contains(&value_string.to_lowercase())
                    } else {
                        loginfo!("Checking if '{}' contains '{}'", row_string, value_string);
                        row_string.contains(&value_string)
                    };
                    
                    loginfo!("String contains result: {}", result);
                    result
                },
                Operator::StringRegularExpression => {
                    loginfo!("Performing regex match for column '{}'", column);
                    
                    let row_string = match row_value {
                        AlbaTypes::Int(i) => i.to_string(),
                        AlbaTypes::Bigint(i) => i.to_string(),
                        AlbaTypes::Float(i) => i.to_string(),
                        AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | 
                        AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s.to_string(),
                        _ => {
                            loginfo!("Invalid type for string operations - Row value: {:?}", row_value);
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
                            loginfo!("Invalid type for string operations - Query value: {:?}", value);
                            return Err(gerr("Invalid, the entered type cannot make string operations"));
                        }
                    };
    
                    loginfo!("Regex pattern: '{}', Target string: '{}'", value_string, row_string);
    
                    let regex_result = if let Some(cached_regex) = regex_cache.get(&value_string) {
                        loginfo!("Using cached regex for pattern '{}'", value_string);
                        let match_result = cached_regex.is_match(&row_string);
                        loginfo!("Regex match result (cached): {}", match_result);
                        match_result
                    } else {
                        loginfo!("Compiling new regex for pattern '{}'", value_string);
                        let re = Regex::new(&value_string);
                        match re {
                            Ok(compiled_regex) => {
                                let match_result = compiled_regex.is_match(&row_string);
                                loginfo!("Regex compiled successfully, match result: {}", match_result);
                                regex_cache.insert(value_string, compiled_regex);
                                match_result
                            },
                            Err(e) => {
                                loginfo!("Regex compilation failed: {}", e);
                                return Err(gerr(&e.to_string()));
                            }
                        }
                    };
                    
                    regex_result
                }
            };
            
            loginfo!("Condition {} evaluation result: {}", index + 1, check);
            
            if let Some(gate) = logical_gate {
                loginfo!("Applying logical gate: {:?}", gate);
                match gate {
                    LogicalGate::And => {
                        if !check {
                            loginfo!("AND gate with false condition, short-circuiting to false");
                            result = false;
                            break;
                        }
                        loginfo!("AND gate with true condition, continuing");
                    },
                    LogicalGate::Or => {
                        if check {
                            loginfo!("OR gate with true condition, short-circuiting to true");
                            result = true;
                            break;
                        }
                        loginfo!("OR gate with false condition, continuing");
                    }
                }
            } else {
                loginfo!("No logical gate, setting result to condition result");
                result = check;
            }
        }
    
        loginfo!("Final row_match result: {}", result);
        Ok(result)
    }
    pub fn query_type(&self) -> Result<QueryType, Error> {
        if self.chain.is_empty()  || self.primary_key.is_none(){
            return Ok(QueryType::Scan);
        }
        let pk = self.primary_key.as_ref().unwrap();
        let mut vector: Vec<(Operator,AlbaTypes)> = Vec::new();

        let mut higher_range = Vec::new();
        let mut lower_range = Vec::new();
        let mut equality = false;

        for i in self.chain.iter(){
            if i.0.column == *pk{
                vector.push((i.0.operator.clone(),i.0.value.clone()))
            }
        }
        
        for i in vector{
            if discriminant(&i.0) == discriminant(&Operator::Equal) || discriminant(&i.0) == discriminant(&Operator::StrictEqual){
                if let AlbaTypes::Bigint(val) = i.1{
                    return Ok(QueryType::Indexed(QueryIndexType::Strict(val.get_index())))
                }
                if let AlbaTypes::Int(val) = i.1{
                    return Ok(QueryType::Indexed(QueryIndexType::Strict(val.get_index())))
                }
                if let AlbaTypes::Float(val) = i.1{
                    return Ok(QueryType::Indexed(QueryIndexType::Strict(val.get_index())))
                }
            }
            if discriminant(&i.0) == discriminant(&Operator::Greater) || discriminant(&i.0) == discriminant(&Operator::GreaterEquality){
                if let AlbaTypes::Bigint(val) = i.1{
                    higher_range.push(val.get_index())
                }
                if let AlbaTypes::Int(val) = i.1{
                    higher_range.push(val.get_index())
                }
                if let AlbaTypes::Float(val) = i.1{
                    higher_range.push(val.get_index())
                }
                equality = (discriminant(&i.0) == discriminant(&Operator::GreaterEquality)) || equality;
            }
            if discriminant(&i.0) == discriminant(&Operator::Lower) || discriminant(&i.0) == discriminant(&Operator::LowerEquality){
                if let AlbaTypes::Bigint(val) = i.1{
                    lower_range.push(val.get_index())
                }
                if let AlbaTypes::Int(val) = i.1{
                    lower_range.push(val.get_index())
                }
                if let AlbaTypes::Float(val) = i.1{
                    lower_range.push(val.get_index())
                }
                equality = (discriminant(&i.0) == discriminant(&Operator::LowerEquality)) || equality;
            }
        }
        let low_val = lower_range.iter().min().copied();
        let high_val = higher_range.iter().max().copied();
        loginfo!("{:?}~{:?}",low_val,high_val);
        let b =match (low_val,high_val) {
            (Some(l),Some(h)) => {
                if !equality{
                    Ok(
                        QueryType::Indexed(QueryIndexType::Range(h..l))
                    )
                }else{
                    Ok(
                        QueryType::Indexed(QueryIndexType::InclusiveRange(h..=l))
                    )
                }
            },
            (Some(l),None) => {
                Ok(
                    if !equality{
                        QueryType::Indexed(QueryIndexType::Range(0..l))
                    }else{
                        QueryType::Indexed(QueryIndexType::InclusiveRange(0..=l))
                    }
                )
            }
            ,
            (None, Some(h)) => {
                Ok(
                    if !equality {
                        QueryType::Indexed(QueryIndexType::InclusiveRange(h..=u64::MAX))
                    } else {
                        QueryType::Indexed(QueryIndexType::InclusiveRange(h..=u64::MAX))
                    }
                )
            },
            _ => {
                Ok(QueryType::Scan)
            }
            
        };
        loginfo!("\n\n\n\n{:?}\n\n\n\n",b);
        b
        
    }
}