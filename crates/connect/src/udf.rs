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

//! UDF registration support for SparkSession

use crate::client::SparkClient;
use crate::errors::SparkError;
use crate::plan::LogicalPlanBuilder;
use crate::spark;
use crate::types::DataType;

/// Interface for registering user-defined functions.
///
/// Access via [`SparkSession::udf`](crate::session::SparkSession::udf).
pub struct UdfRegistration {
    client: SparkClient,
}

impl UdfRegistration {
    pub(crate) fn new(client: SparkClient) -> Self {
        Self { client }
    }

    /// Register a Java UDF by class name.
    ///
    /// The Java class must implement `org.apache.spark.sql.api.java.UDF1` (or UDF2, etc.)
    /// and be on the classpath (e.g., via [`SparkSession::add_artifact`]).
    ///
    /// # Arguments
    /// * `name` - Name to register the UDF under
    /// * `class_name` - Fully qualified Java class name
    /// * `return_type` - Return type of the UDF
    pub async fn register_java(
        &self,
        name: &str,
        class_name: &str,
        return_type: DataType,
    ) -> Result<(), SparkError> {
        let udf = spark::CommonInlineUserDefinedFunction {
            function_name: name.to_string(),
            deterministic: true,
            arguments: vec![],
            function: Some(
                spark::common_inline_user_defined_function::Function::JavaUdf(spark::JavaUdf {
                    class_name: class_name.to_string(),
                    output_type: Some(return_type.to_proto_type()),
                    aggregate: false,
                }),
            ),
        };

        let cmd = spark::command::CommandType::RegisterFunction(udf);
        let plan = LogicalPlanBuilder::plan_cmd(cmd);

        let mut client = self.client.clone();
        client.execute_command_and_fetch(plan).await?;

        Ok(())
    }

    /// Register a Java UDAF (User-Defined Aggregate Function) by class name.
    ///
    /// The Java class must implement `org.apache.spark.sql.expressions.UserDefinedAggregateFunction`
    /// and be on the classpath.
    ///
    /// # Arguments
    /// * `name` - Name to register the UDAF under
    /// * `class_name` - Fully qualified Java class name
    pub async fn register_java_udaf(&self, name: &str, class_name: &str) -> Result<(), SparkError> {
        let udf = spark::CommonInlineUserDefinedFunction {
            function_name: name.to_string(),
            deterministic: true,
            arguments: vec![],
            function: Some(
                spark::common_inline_user_defined_function::Function::JavaUdf(spark::JavaUdf {
                    class_name: class_name.to_string(),
                    output_type: None,
                    aggregate: true,
                }),
            ),
        };

        let cmd = spark::command::CommandType::RegisterFunction(udf);
        let plan = LogicalPlanBuilder::plan_cmd(cmd);

        let mut client = self.client.clone();
        client.execute_command_and_fetch(plan).await?;

        Ok(())
    }

    /// Build the protobuf command for a Java UDF registration (for testing).
    #[cfg(test)]
    fn build_register_java_cmd(
        name: &str,
        class_name: &str,
        return_type: DataType,
    ) -> spark::CommonInlineUserDefinedFunction {
        spark::CommonInlineUserDefinedFunction {
            function_name: name.to_string(),
            deterministic: true,
            arguments: vec![],
            function: Some(
                spark::common_inline_user_defined_function::Function::JavaUdf(spark::JavaUdf {
                    class_name: class_name.to_string(),
                    output_type: Some(return_type.to_proto_type()),
                    aggregate: false,
                }),
            ),
        }
    }

    /// Build the protobuf command for a Java UDAF registration (for testing).
    #[cfg(test)]
    fn build_register_java_udaf_cmd(
        name: &str,
        class_name: &str,
    ) -> spark::CommonInlineUserDefinedFunction {
        spark::CommonInlineUserDefinedFunction {
            function_name: name.to_string(),
            deterministic: true,
            arguments: vec![],
            function: Some(
                spark::common_inline_user_defined_function::Function::JavaUdf(spark::JavaUdf {
                    class_name: class_name.to_string(),
                    output_type: None,
                    aggregate: true,
                }),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_java_udf_proto_structure() {
        let udf = UdfRegistration::build_register_java_cmd(
            "my_udf",
            "com.example.MyUDF",
            DataType::String,
        );

        assert_eq!(udf.function_name, "my_udf");
        assert!(udf.deterministic);
        assert!(udf.arguments.is_empty());

        match udf.function {
            Some(spark::common_inline_user_defined_function::Function::JavaUdf(ref java)) => {
                assert_eq!(java.class_name, "com.example.MyUDF");
                assert!(!java.aggregate);
                assert!(java.output_type.is_some());
            }
            _ => panic!("Expected JavaUdf function variant"),
        }
    }

    #[test]
    fn test_register_java_udf_return_type() {
        let udf = UdfRegistration::build_register_java_cmd(
            "int_udf",
            "com.example.IntUDF",
            DataType::Integer,
        );

        match udf.function {
            Some(spark::common_inline_user_defined_function::Function::JavaUdf(ref java)) => {
                let output = java.output_type.as_ref().unwrap();
                assert!(matches!(
                    output.kind,
                    Some(spark::data_type::Kind::Integer(_))
                ));
            }
            _ => panic!("Expected JavaUdf"),
        }
    }

    #[test]
    fn test_register_java_udaf_proto_structure() {
        let udf = UdfRegistration::build_register_java_udaf_cmd("my_udaf", "com.example.MyUDAF");

        assert_eq!(udf.function_name, "my_udaf");
        assert!(udf.deterministic);

        match udf.function {
            Some(spark::common_inline_user_defined_function::Function::JavaUdf(ref java)) => {
                assert_eq!(java.class_name, "com.example.MyUDAF");
                assert!(java.aggregate);
                assert!(java.output_type.is_none());
            }
            _ => panic!("Expected JavaUdf function variant with aggregate=true"),
        }
    }

    #[test]
    fn test_register_java_udf_creates_valid_command() {
        let udf =
            UdfRegistration::build_register_java_cmd("test_fn", "com.test.Fn", DataType::Double);

        let cmd = spark::command::CommandType::RegisterFunction(udf);
        let plan = LogicalPlanBuilder::plan_cmd(cmd);

        // Verify the plan has a command
        assert!(plan.op_type.is_some());
        match plan.op_type {
            Some(spark::plan::OpType::Command(ref command)) => {
                assert!(matches!(
                    command.command_type,
                    Some(spark::command::CommandType::RegisterFunction(_))
                ));
            }
            _ => panic!("Expected Command plan"),
        }
    }
}
