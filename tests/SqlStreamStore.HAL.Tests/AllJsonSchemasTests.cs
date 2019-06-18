namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Shouldly;
    using Xunit;

    public class AllJsonSchemasTests
    {
        private static Regex s_isSqlStreamStoreSchema = new Regex(@"Schema\.(.*)\.schema\.json$");

        private static byte[] s_bom =
        {
            0xEF, 0xBB, 0xBF
        };

        private static StreamReader GetStreamReader(string manifestName)
            => new StreamReader(GetStream(manifestName));

        private static Stream GetStream(string manifestName)
            => typeof(SchemaSet<>)
                .GetTypeInfo()
                .Assembly
                .GetManifestResourceStream(manifestName);

        public static IEnumerable<object[]> GetJsonSchemas() => from manifestName in typeof(SchemaSet<>)
                .GetTypeInfo()
                .Assembly
                .GetManifestResourceNames()
            where s_isSqlStreamStoreSchema.IsMatch(manifestName)
            select new[] { manifestName };


        [Theory, MemberData(nameof(GetJsonSchemas))]
        public async Task byte_order_mark_not_present(string manifestName)
        {
            byte[] firstThreeBytes = new byte[3];

            using(var stream = GetStream(manifestName))
            {
                await stream.ReadAsync(firstThreeBytes);

                firstThreeBytes.SequenceEqual(s_bom)
                    .ShouldBeFalse();
            }
        }

        [Theory, MemberData(nameof(GetJsonSchemas))]
        public void json_schema_is_compatible_with_markdown_generator(string manifestName)
        {
            using(var reader = GetStreamReader(manifestName))
            {
                JsonConvert.DeserializeObject<JObject>(reader.ReadToEnd()).Value<string>("$schema")
                    .ShouldBe("http://json-schema.org/draft-07/schema#");
            }
        }
    }
}