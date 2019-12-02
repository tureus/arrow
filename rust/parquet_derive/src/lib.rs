// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![recursion_limit = "128"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

extern crate parquet;

use syn::{parse_macro_input, Data, DataStruct, DeriveInput};

mod parquet_field;

/// Derive flat, simple RecordWriter implementations. Works by parsing
/// a struct tagged with `#[derive(ParquetRecordWriter)]` and emitting
/// the correct writing code for each field of the struct. Column writers
/// are generated in the order they are defined.
///
/// It is up to the programmer to keep the order of the struct
/// fields lined up with the schema.
///
/// Example:
///
/// ```ignore
/// use parquet;
/// use parquet::record::RecordWriter;
/// use parquet::schema::parser::parse_message_type;
///
/// use std::rc::Rc;
//
/// #[derive(ParquetRecordWriter)]
/// struct ACompleteRecord<'a> {
///   pub a_bool: bool,
///   pub a_str: &'a str,
/// }
///
/// let schema_str = "message schema {
///   REQUIRED boolean         a_bool;
///   REQUIRED BINARY          a_str (UTF8);
/// }";
///
/// pub fn write_some_records() {
///   let samples = vec![
///     ACompleteRecord {
///       a_bool: true,
///       a_str: "I'm true"
///     },
///     ACompleteRecord {
///       a_bool: false,
///       a_str: "I'm false"
///     }
///   ];
///
///  let schema = Rc::new(parse_message_type(schema_str).unwrap());
///
///  let props = Rc::new(WriterProperties::builder().build());
///  let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
///
///  let mut row_group = writer.next_row_group().unwrap();
///  samples.as_slice().write_to_row_group(&mut row_group).unwrap();
///  writer.close_row_group(row_group).unwrap();
///  writer.close().unwrap();
/// }
/// ```
///
#[proc_macro_derive(ParquetRecordWriter)]
pub fn parquet_record_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let fields = match input.data {
        Data::Struct(DataStruct { fields, .. }) => fields,
        Data::Enum(_) => unimplemented!("don't support enum"),
        Data::Union(_) => unimplemented!("don't support union"),
    };

    let field_infos: Vec<_> = fields
        .iter()
        .map(|f: &syn::Field| parquet_field::Field::from(f))
        .collect();

    let writer_snippets: Vec<proc_macro2::TokenStream> =
        field_infos.iter().map(|x| x.writer_snippet()).collect();

    let derived_for = input.ident;
    let generics = input.generics;

    (quote! {
    impl#generics RecordWriter<#derived_for#generics> for &[#derived_for#generics] {
      fn write_to_row_group(&self, row_group_writer: &mut Box<parquet::file::writer::RowGroupWriter>) -> Result<(), parquet::errors::ParquetError> {
        let mut row_group_writer = row_group_writer;
        let records = &self; // Used by all the writer snippets to be more clear

        #(
          {
              let mut some_column_writer = row_group_writer.next_column().unwrap();
              if let Some(mut column_writer) = some_column_writer {
                  #writer_snippets
                  row_group_writer.close_column(column_writer)?;
              } else {
                  return Err(parquet::errors::ParquetError::General("failed to get next column".into()))
              }
          }
        );*

        Ok(())
      }
    }
  }).into()
}

#[proc_macro_derive(ParquetRecordSchema)]
pub fn parquet_record_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let fields = match input.data {
        Data::Struct(DataStruct { fields, .. }) => fields,
        Data::Enum(_) => unimplemented!("don't support enum"),
        Data::Union(_) => unimplemented!("don't support union"),
    };

    let field_infos: Vec<_> = fields
        .iter()
        .map(|f: &syn::Field| parquet_field::Field::from(f))
        .collect();

    //    panic!(
    //        "{:#?}\n\n\n{}\n\n\n{}",
    //        field_infos.last().unwrap(),
    //        field_infos.last().unwrap().last_part(),
    //        field_infos.last().unwrap().last_part() == "Uuid"
    //    );

    use parquet::basic::Type;
    let physical_types: Vec<_> = field_infos
        .iter()
        .map(|field| match field.physical_type() {
            Type::BOOLEAN => {
                quote! { parquet::basic::Type::BOOLEAN }
            }
            Type::INT32 => {
                quote! { parquet::basic::Type::INT32 }
            }
            Type::INT64 => {
                quote! { parquet::basic::Type::INT64 }
            }
            Type::INT96 => {
                quote! { parquet::basic::Type::INT96 }
            }
            Type::FLOAT => {
                quote! { parquet::basic::Type::FLOAT }
            }
            Type::DOUBLE => {
                quote! { parquet::basic::Type::DOUBLE }
            }
            Type::BYTE_ARRAY => {
                quote! { parquet::basic::Type::BYTE_ARRAY }
            }
            Type::FIXED_LEN_BYTE_ARRAY => {
                quote! { parquet::basic::Type::FIXED_LEN_BYTE_ARRAY }
            }
        })
        .collect();

    let logical_types: Vec<Option<proc_macro2::TokenStream>> = field_infos
        .iter()
        .map(|f| {
            if f.last_part() == "str" || f.last_part() == "String" || f.last_part() == "Uuid" {
                Some(quote!{ .with_logical_type(parquet::basic::LogicalType::UTF8) })
            } else if f.last_part() == "NaiveDateTime" {
                Some(quote!{ .with_logical_type(parquet::basic::LogicalType::TIMESTAMP_MILLIS) })
            } else if f.last_part() == "NaiveDate" {
                Some(quote!{ .with_logical_type(parquet::basic::LogicalType::DATE) })
            } else {
                None
            }
        })
        .collect();

    let repetition_levels: Vec<_> = field_infos
        .iter()
        .map(|f| {
            if f.definition_levels() == 0 {
                quote! { parquet::basic::Repetition::REQUIRED }
            } else {
                quote! { parquet::basic::Repetition::OPTIONAL }
            }
        })
        .collect();

    // field.ident()
    let field_identifiers: Vec<_> =
        field_infos.iter().map(|field| field.ident()).collect();

    let derived_for = input.ident;
    let generics = input.generics;

    (quote! {
      impl#generics RecordSchema for #derived_for#generics {
        fn schema() -> parquet::schema::types::Type {

          let mut fields = vec![
            #(
                std::rc::Rc::new(
                    parquet::schema::types::PrimitiveTypeBuilder::new(
                        stringify!(#field_identifiers),
                        #physical_types
                    )
                    .with_repetition(#repetition_levels)
                    #logical_types
                    .build()
                    .expect("schema builder failed on a type")
                )
            ),*
          ];

          let group_type_builder = parquet::schema::types::GroupTypeBuilder::new("schema").with_fields(&mut fields);

          group_type_builder.build().expect("could not build parquet schema")
        }
      }
    })
    .into()
}
