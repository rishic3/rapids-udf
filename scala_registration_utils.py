from typing import Optional

from py4j.java_gateway import JavaClass, JavaMember
from pyspark.sql import SparkSession

REGISTRY_OBJECT_NAME = "UDFRegistry"
CPU_REGISTER_METHOD_NAME = "registerUDF"
RAPIDS_REGISTER_METHOD_NAME = "registerRapidsUDF"


def registerScalaUDF(
    spark: SparkSession,
    udf_name: str,
    fully_qualified_class_name: str,
    registry_object_name: Optional[str] = REGISTRY_OBJECT_NAME,
):
    """
    Register a Scala UDF under udf_name by calling the register_method_name on the class_path.

    Args:
        spark (SparkSession): The Spark session to use.
        udf_name (str): The name of the UDF to register.
        fully_qualified_class_name (str): The fully qualified class name of the Scala UDF.
        registry_object_name (Optional[str]): The name of the registry object to use, defaults to REGISTRY_OBJECT_NAME.
                                              This should exist on the same class path as the UDF.
    """
    base_package = ".".join(fully_qualified_class_name.split(".")[:-1])
    helper_object_class_path = f"{base_package}.{registry_object_name}"

    class_name = fully_qualified_class_name.split(".")[-1]

    # Determine which Scala registration method to call based on the UDF class name.
    if "rapids" in class_name.lower():
        register_method_name = RAPIDS_REGISTER_METHOD_NAME
        print(f"Calling {register_method_name} to register {class_name} on GPU")
    else:
        register_method_name = CPU_REGISTER_METHOD_NAME
        print(f"Calling {register_method_name} to register {class_name} on CPU")

    # Get helper class package from JVM
    helper_obj = _get_spark_java_class(spark, helper_object_class_path)

    # Retrieve the method (JavaMember)
    registration_method = getattr(helper_obj, register_method_name)
    if not isinstance(registration_method, JavaMember):
        raise ValueError(
            f"Expected {registration_method} to be a JavaMember, but got: {type(registration_method)}"
        )

    # Call the registration method on the JVM with args (spark: SparkSession, udfName: String)
    registration_method(spark._jsparkSession, udf_name)
    print(
        f"Successfully registered UDF as '{udf_name}' from '{fully_qualified_class_name}'."
    )


def _get_spark_java_class(
    spark: SparkSession, fully_qualified_class_name: str
) -> JavaClass:
    """
    Get the Java object from the Spark JVM by following the class path (e.g., com.example.udf.MyUDF)
    """
    jvm = spark._jvm
    if jvm is None:
        raise ValueError("Could not retrieve JVMView from Spark Session")

    obj = eval("spark._jvm." + fully_qualified_class_name)

    if not isinstance(obj, JavaClass):
        raise ValueError(
            f"The fully qualified class name {fully_qualified_class_name} does not lead to a Java Class on the JVM, got type {type(obj)}"
        )

    return obj
