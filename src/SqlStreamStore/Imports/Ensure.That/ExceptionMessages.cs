namespace SqlStreamStore.Imports.Ensure.That
{
    public class ExceptionMessages
    {
        public static string Common_IsNotNull_Failed { get; private set; } = "Value can not be null.";

        public static string Booleans_IsTrueFailed { get; private set; } = "Expected an expression that evaluates to true.";
        public static string Booleans_IsFalseFailed { get; private set; } = "Expected an expression that evaluates to false.";

        public static string Collections_Any_Failed { get; private set; } = "The predicate did not match any elements.";
        public static string Collections_ContainsKey_Failed { get; private set; } = "Key '{0}' does not exist.";
        public static string Collections_HasItemsFailed { get; private set; } = "Empty collection is not allowed.";
        public static string Collections_SizeIs_Failed { get; private set; } = "Expected size '{0}' but found '{1}'.";

        public static string Comp_Is_Failed { get; private set; } = "Value '{0}' is not '{1}'.";
        public static string Comp_IsNot_Failed { get; private set; } = "Value '{0}' is '{1}', which was not expected.";
        public static string Comp_IsNotLt { get; private set; } = "value '{0}' is not lower than limit '{1}'.";
        public static string Comp_IsNotLte { get; private set; } = "value '{0}' is not lower than or equal to limit '{1}'.";
        public static string Comp_IsNotGt { get; private set; } = "value '{0}' is not greater than limit '{1}'.";
        public static string Comp_IsNotGte { get; private set; } = "value '{0}' is not greater than or equal to limit '{1}'.";
        public static string Comp_IsNotInRange_ToLow { get; private set; } = "value '{0}' is < min '{1}'.";
        public static string Comp_IsNotInRange_ToHigh { get; private set; } = "value '{0}' is > max '{1}'.";

        public static string Guids_IsNotEmpty_Failed { get; private set; } = "Empty Guid is not allowed.";

        public static string Strings_IsEqualTo_Failed { get; private set; } = "Value '{0}' is not '{1}'.";
        public static string Strings_IsNotEqualTo_Failed { get; private set; } = "Value '{0}' is '{1}', which was not expected.";
        public static string Strings_SizeIs_Failed { get; private set; } = "Expected length '{0}' but found '{1}'.";
        public static string Strings_IsNotNullOrWhiteSpace_Failed { get; private set; } = "The string can't be left empty, null or consist of only whitespaces.";
        public static string Strings_IsNotNullOrEmpty_Failed { get; private set; } = "The string can't be null or empty.";
        public static string Strings_HasLengthBetween_Failed_ToShort { get; private set; } = "The string is not long enough. Must be between '{0}' and '{1}' but was '{2}' characters long.";
        public static string Strings_HasLengthBetween_Failed_ToLong { get; private set; } = "The string is too long. Must be between '{0}' and  '{1}'. Must be between '{0}' and '{1}' but was '{2}' characters long.";
        public static string Strings_Matches_Failed { get; private set; } = "value '{0}' does not match '{1}'";
        public static string Strings_IsNotEmpty_Failed { get; private set; } = "Empty String is not allowed.";
        public static string Strings_IsGuid_Failed { get; private set; } = "Value '{0}' is not a valid GUID.";

        public static string Types_IsOfType_Failed { get; private set; } = "The param is not of expected type. Expected: '{0}'. Got: '{1}'.";
        public static string Types_IsNotOfType_Failed { get; private set; } = "The param was expected to not be of the type: '{0}'. But it was.";
        public static string Types_IsClass_Failed_Null { get; private set; } = "The param was expected to be a class, but was NULL.";
        public static string Types_IsClass_Failed { get; private set; } = "The param was expected to be a class, but was type of: '{0}'.";

        public static string ValueTypes_IsNotDefault_Failed { get; private set; } = "The param was expected to not be of default value.";
    }
}