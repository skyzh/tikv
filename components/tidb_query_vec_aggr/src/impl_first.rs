// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::EvalType;
use tipb::{Expr, ExprType, FieldType};

use super::*;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::expr::EvalContext;
use tidb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// The parser for FIRST aggregate function.
pub struct AggrFnDefinitionParserFirst;

impl <'a> super::AggrDefinitionParser<'a> for AggrFnDefinitionParserFirst {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::First);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    fn parse(
        &'a self,
        mut aggr_def: Expr,
        ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction<'a> + 'a>> {
        use std::convert::TryFrom;
        use tidb_query_datatype::FieldTypeAccessor;

        assert_eq!(aggr_def.get_tp(), ExprType::First);
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let eval_type = EvalType::try_from(child.get_field_type().as_accessor().tp()).unwrap();

        let out_ft = aggr_def.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        if out_et != eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // FIRST outputs one column with the same type as its child
        out_schema.push(out_ft);
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            ctx,
            src_schema.len(),
        )?);

        match_template::match_template! {
            TT = [Int, Real, Duration, Decimal, DateTime],
            match eval_type {
                EvalType::TT => Ok(Box::new(AggrFnFirst::<'_, &TT>::new())),
                EvalType::Json => Ok(Box::new(AggrFnFirst::<'_, JsonRef>::new())),
                EvalType::Bytes => Ok(Box::new(AggrFnFirst::<'_, BytesRef>::new())),
            }
        }
    }
}

/// The FIRST aggregate function.
// #[derive(Debug, AggrFunction)]
// #[aggr_function(state = AggrFnStateFirst::<'_, T>::new())]
#[derive(Debug)]
pub struct AggrFnFirst<'a, T>(PhantomData<&'a T>)
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>;

impl<'a, T> crate::AggrFunction<'a> for AggrFnFirst<'a, T>
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    #[inline]
    fn name(&self) -> &'static str {
        "AggrFnFirst"
    }
    #[inline]
    fn create_state(&self) -> Box<dyn crate::AggrFunctionState<'a> + 'a> {
        Box::new(AggrFnStateFirst::<'a, T>::new())
    }
}

impl<'a, T> AggrFnFirst<'a, T>
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn new() -> Self {
        AggrFnFirst(PhantomData)
    }
}

/// The state of the FIRST aggregate function.
#[derive(Debug)]
pub enum AggrFnStateFirst<'a, T>
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    Empty,
    Valued(Option<T::EvaluableType>),
}

impl<'a, T> AggrFnStateFirst<'a, T>
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    pub fn new() -> Self {
        AggrFnStateFirst::Empty
    }
}

// Here we manually implement `AggrFunctionStateUpdatePartial` instead of implementing
// `ConcreteAggrFunctionState` so that `update_repeat` and `update_vector` can be faster.
impl<'a, T> super::AggrFunctionStateUpdatePartial<'a, T> for AggrFnStateFirst<'a, T>
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    // ChunkedType has been implemented in AggrFunctionStateUpdatePartial<T1> for AggrFnStateFirst<T2>

    #[inline]
    unsafe fn update_unsafe(&mut self, _ctx: &mut EvalContext, value: Option<T>) -> Result<()> {
        if let AggrFnStateFirst::Empty = self {
            // TODO: avoid this clone
            *self = AggrFnStateFirst::Valued(value.map(|x| x.to_owned_value()));
        }
        Ok(())
    }

    #[inline]
    unsafe fn update_repeat_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        assert!(repeat_times > 0);
        self.update_unsafe(ctx, value)
    }

    #[inline]
    unsafe fn update_vector_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        _phantom_data: Option<T>,
        physical_values: T::ChunkedType,
        logical_rows: &[usize],
    ) -> Result<()> {
        if let Some(physical_index) = logical_rows.first() {
            self.update_unsafe(ctx, physical_values.get_option_ref(*physical_index))?;
        }
        Ok(())
    }
}

// In order to make `AggrFnStateFirst` satisfy the `AggrFunctionState` trait, we default impl all
// `AggrFunctionStateUpdatePartial` of `Evaluable` for all `AggrFnStateFirst`.
impl<'a, 'b, T1, T2> super::AggrFunctionStateUpdatePartial<'a, T1> for AggrFnStateFirst<'b, T2>
where
    T1: EvaluableRef<'a> + 'a,
    T2: EvaluableRef<'b> + 'b,
    VectorValue: VectorValueExt<T2::EvaluableType>,
{
    #[inline]
    default unsafe fn update_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _value: Option<T1>,
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default unsafe fn update_repeat_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _value: Option<T1>,
        _repeat_times: usize,
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default unsafe fn update_vector_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _phantom_data: Option<T1>,
        _physical_values: T1::ChunkedType,
        _logical_rows: &[usize],
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }
}

impl<'a, T> super::AggrFunctionState<'a> for AggrFnStateFirst<'a, T>
where
    T: EvaluableRef<'a> + 'a,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        let res = if let AggrFnStateFirst::Valued(v) = self {
            v.clone()
        } else {
            None
        };
        target[0].push(res);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use tidb_query_datatype::FieldTypeTp;
    use tipb_helper::ExprDefBuilder;

    use crate::AggrDefinitionParser;

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<&'static Int>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        update!(state, &mut ctx, Some(&1)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None, Some(1)]);

        update!(state, &mut ctx, Some(&2)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None, Some(1), Some(1)]);
    }

    #[test]
    fn test_update_repeat() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<BytesRef<'static>>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Bytes)];

        update_repeat!(state, &mut ctx, Some(&[1u8] as BytesRef), 2).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_bytes_slice(), &[Some(vec![1])]);

        update_repeat!(state, &mut ctx, Some(&[2u8] as BytesRef), 3).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_bytes_slice(), &[Some(vec![1]), Some(vec![1])]);
    }

    #[test]
    fn test_update_vector() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<&'static Int>::new();
        let mut state = function.create_state();
        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        update_vector!(
            state,
            &mut ctx,
            &NotChunkedVec::from_slice(&[Some(0); 0]),
            &[]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        result[0].clear();
        update_vector!(state, &mut ctx, &NotChunkedVec::from_slice(&[Some(1)]), &[]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            &NotChunkedVec::from_slice(&[None, Some(2)]),
            &[0, 1]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            &NotChunkedVec::from_slice(&[Some(1)]),
            &[0]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        // Reset state
        let mut state = function.create_state();

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            &NotChunkedVec::from_slice(&[None, Some(2)]),
            &[1, 0]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(2)]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::First, FieldTypeTp::Double) // Expect LongLong but give Double
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        AggrFnDefinitionParserFirst.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserFirst
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
