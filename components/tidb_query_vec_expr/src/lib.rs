// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with TiDB pushed down executors.
//!
//! The query engine is able to scan and understand rows stored by TiDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! TiKV Coprocessor interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn)]
#![feature(test)]
#![feature(int_error_matching)]
#![feature(const_loop)]
#![feature(const_if_match)]
#![feature(ptr_offset_from)]

#[macro_use(box_err, box_try, try_opt)]
extern crate tikv_util;

#[macro_use(other_err)]
extern crate tidb_query_common;

#[cfg(test)]
extern crate test;

pub mod types;

pub mod impl_control;
pub mod impl_encryption;
pub mod impl_json;

pub use self::types::*;

use tidb_query_datatype::{Collation, FieldTypeAccessor, FieldTypeFlag};
use tipb::{Expr, FieldType, ScalarFuncSig};

use tidb_query_common::Result;
use tidb_query_datatype::codec::collation::*;
use tidb_query_datatype::codec::data_type::*;

use self::impl_control::*;
use self::impl_encryption::*;
use self::impl_json::*;

#[rustfmt::skip]
fn map_expr_node_to_rpn_func(expr: &Expr) -> Result<RpnFnMeta> {
    let value = expr.get_sig();
    let children = expr.get_children();
    let ft = expr.get_field_type();
    Ok(match value {
        // impl_control
        ScalarFuncSig::IfNullInt => if_null_fn_meta::<Int>(),
        ScalarFuncSig::IfNullReal => if_null_fn_meta::<Real>(),
        ScalarFuncSig::IfNullString => if_null_fn_meta::<Bytes>(),
        ScalarFuncSig::IfNullDecimal => if_null_fn_meta::<Decimal>(),
        ScalarFuncSig::IfNullTime => if_null_fn_meta::<DateTime>(),
        ScalarFuncSig::IfNullDuration => if_null_fn_meta::<Duration>(),
        ScalarFuncSig::IfNullJson => if_null_fn_meta::<Json>(),
        ScalarFuncSig::IfInt => if_condition_fn_meta::<Int>(),
        ScalarFuncSig::IfReal => if_condition_fn_meta::<Real>(),
        ScalarFuncSig::IfDecimal => if_condition_fn_meta::<Decimal>(),
        ScalarFuncSig::IfTime => if_condition_fn_meta::<DateTime>(),
        ScalarFuncSig::IfString => if_condition_fn_meta::<Bytes>(),
        ScalarFuncSig::IfDuration => if_condition_fn_meta::<Duration>(),
        ScalarFuncSig::IfJson => if_condition_fn_meta::<Json>(),
        ScalarFuncSig::CaseWhenInt => case_when_fn_meta::<Int>(),
        ScalarFuncSig::CaseWhenReal => case_when_fn_meta::<Real>(),
        ScalarFuncSig::CaseWhenString => case_when_fn_meta::<Bytes>(),
        ScalarFuncSig::CaseWhenDecimal => case_when_fn_meta::<Decimal>(),
        ScalarFuncSig::CaseWhenTime => case_when_fn_meta::<DateTime>(),
        ScalarFuncSig::CaseWhenDuration => case_when_fn_meta::<Duration>(),
        ScalarFuncSig::CaseWhenJson => case_when_fn_meta::<Json>(),
        // impl_encryption
        ScalarFuncSig::UncompressedLength => uncompressed_length_fn_meta(),
        ScalarFuncSig::Md5 => md5_fn_meta(),
        ScalarFuncSig::Sha1 => sha1_fn_meta(),
        ScalarFuncSig::Sha2 => sha2_fn_meta(),
        ScalarFuncSig::RandomBytes => random_bytes_fn_meta(),
        // impl_json
        ScalarFuncSig::JsonDepthSig => json_depth_fn_meta(),
        ScalarFuncSig::JsonTypeSig => json_type_fn_meta(),
        ScalarFuncSig::JsonSetSig => json_set_fn_meta(),
        ScalarFuncSig::JsonReplaceSig => json_replace_fn_meta(),
        ScalarFuncSig::JsonInsertSig => json_insert_fn_meta(),
        ScalarFuncSig::JsonArraySig => json_array_fn_meta(),
        ScalarFuncSig::JsonObjectSig => json_object_fn_meta(),
        ScalarFuncSig::JsonMergeSig => json_merge_fn_meta(),
        ScalarFuncSig::JsonUnquoteSig => json_unquote_fn_meta(),
        ScalarFuncSig::JsonExtractSig => json_extract_fn_meta(),
        ScalarFuncSig::JsonLengthSig => json_length_fn_meta(),
        ScalarFuncSig::JsonRemoveSig => json_remove_fn_meta(),
        ScalarFuncSig::JsonKeysSig => json_keys_fn_meta(),
        ScalarFuncSig::JsonKeys2ArgsSig => json_keys_fn_meta(),
        _ => return Err(other_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            value
        )),
    })
}
