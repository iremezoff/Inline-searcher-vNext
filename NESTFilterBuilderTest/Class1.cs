using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using NESTFilterBuilder;
using NUnit.Framework;

namespace NESTFilterBuilderTest
{
    public class DataControllerTest
    {
        private DataController _target;

        [SetUp]
        public void Init()
        {
            _target = new DataController();
            _target.Configuration = new HttpConfiguration();
            _target.Request = new HttpRequestMessage();
        }

        [Test]
        public void Get_WhenQueryStringAndCurrentItemAreNull_ThenThereIsListOfFirstLevel()
        {
            var response = _target.Get().ExecuteAsync(new CancellationToken()).Result;

            response.EnsureSuccessStatusCode();

            var result = response.Content.ReadAsAsync<IEnumerable<SearchResult>>().Result;

            CollectionAssert.IsNotEmpty(result);

            CollectionAssert.AreEquivalent(Enumerable.Repeat(1, result.Count()), result.Select(r => r.Level));
        }

        [Test]
        public void Get_WhenCurrentItemIsNullAndQueryStringIsNotNull_ThenThereIsFitResultWithAllLevels()
        {
            var response = _target.Get(queryString: "Ленин").ExecuteAsync(new CancellationToken()).Result;

            response.EnsureSuccessStatusCode();

            var result = response.Content.ReadAsAsync<IEnumerable<SearchResult>>().Result;

            CollectionAssert.IsNotEmpty(result);

            Assert.AreNotEqual(1, result.Count());
        }

        [Test]
        public void Get_WhenQueryStringContainsHouseMarkThenResponseHasSegmentsWithCurrentItemsGuidAndLevelAndOwnTitle()
        {
            Guid currentItem = Guid.Parse("59CE932D-3AC3-4734-98B5-644E3BE5CF69");
            int level = 7;
            string queryString = "д.13";
            var currentItemRaw = "ул. Ленина";

            var response = _target.Get(currentItem, currentItemRaw, level, queryString).ExecuteAsync(new CancellationToken()).Result;

            response.EnsureSuccessStatusCode();

            var result = response.Content.ReadAsAsync<IEnumerable<SearchResult>>().Result;

            CollectionAssert.IsNotEmpty(result);

            var resultItem = result.Single();

            Assert.AreEqual(level, resultItem.Level);
            Assert.AreEqual(currentItem, resultItem.Value);
            StringAssert.Contains("д. 13", resultItem.Label);
            //StringAssert.AreNotEqualIgnoringCase("д. 13", resultItem.Label);

            var bag = resultItem.Hierarchy.Single();
            Assert.AreEqual(currentItem, bag.Value);
            Assert.AreEqual(level, bag.Level);
            Assert.AreEqual("д. 13", bag.Label);
        }

        [Test]
        public void Get_WhenQueryStringContainsFlatMarkOnlyThenResponseHasNoSegmentForFlat()
        {
            Guid currentItem = Guid.Parse("59CE932D-3AC3-4734-98B5-644E3BE5CF69");
            int level = 7;
            string queryString = "кв.13";
            var currentItemRaw = "ул. Ленина";

            var response = _target.Get(currentItem, currentItemRaw, level, queryString).ExecuteAsync(new CancellationToken()).Result;

            response.EnsureSuccessStatusCode();

            var result = response.Content.ReadAsAsync<IEnumerable<SearchResult>>().Result;

            CollectionAssert.IsEmpty(result);
        }

        [Test]
        public void Get_WhenQueryStringContainsFlatMarkAndItemRawHasHouseMarkThenResponseHasSegmentWithCurrentItemsGuidAndLevelAndOwnTitle()
        {
            Guid currentItem = Guid.Parse("59CE932D-3AC3-4734-98B5-644E3BE5CF69");
            int level = 7;
            string queryString = "кв. 27";
            var currentItemRaw = "д. 13";

            var response = _target.Get(currentItem, currentItemRaw, level, queryString).ExecuteAsync(new CancellationToken()).Result;

            response.EnsureSuccessStatusCode();

            var result = response.Content.ReadAsAsync<IEnumerable<SearchResult>>().Result;

            CollectionAssert.IsNotEmpty(result);

            var resultItem = result.Single();

            Assert.AreEqual(level, resultItem.Level);
            Assert.AreEqual(currentItem, resultItem.Value);
            StringAssert.Contains(queryString, resultItem.Label);
            //StringAssert.AreNotEqualIgnoringCase(queryString, resultItem.Label);
            //StringAssert.Contains(currentItemRaw[0], resultItem.Label);
            //StringAssert.Contains(currentItemRaw[1], resultItem.Label);

            var bag = resultItem.Hierarchy.Single();
            Assert.AreEqual(currentItem, bag.Value);
            Assert.AreEqual(level, bag.Level);
            Assert.AreEqual(queryString, bag.Label);
        }

        [Test]
        public void Get_WhenLastItemIsHouseAndQueryStringHasHouseToo_ThenThereIsNoResult()
        {
            Guid currentItem = Guid.Parse("59CE932D-3AC3-4734-98B5-644E3BE5CF69");
            int level = 7;
            string queryString = "д.27";
            var currentItemRaw = "д. 13";

            var response = _target.Get(currentItem, currentItemRaw, level, queryString).ExecuteAsync(new CancellationToken()).Result;

            Assert.AreNotEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public void Get_SuperTest()
        {
            Guid currentItem = Guid.Parse("59CE932D-3AC3-4734-98B5-644E3BE5CF69");
            int level = 7;
            string queryString = "27";
            var currentItemRaw = "корп. 13";

            var response = _target.Get(currentItem, currentItemRaw, level, queryString).ExecuteAsync(new CancellationToken()).Result;

            response.EnsureSuccessStatusCode();

            var result = response.Content.ReadAsAsync<IEnumerable<SearchResult>>().Result;
        }

        [Test]
        [TestCase("д. 10 4", "д. 10, кв. 4")]
        [TestCase("д. 10", "д. 10")]
        [TestCase("дом 11", "д. 11")]
        [TestCase("д 12", "д. 12")]
        [TestCase("д 12в", "д. 12в")]
        [TestCase("д. 12в", "д. 12в")]
        [TestCase("д 12/3", "д. 12/3")]
        //[TestCase("д 12-3", "д. 12-3")]
        [TestCase("д 13, кв. 33", "д. 13, кв. 33")]
        [TestCase("д 13, кв. 33", "д. 13, кв. 33")]
        [TestCase("д13, кв33", "д. 13, кв. 33")]
        [TestCase("13, 33", "д. 13, кв. 33")]
        [TestCase("13 33", "д. 13, кв. 33")]
        [TestCase("13-33", "д. 13, кв. 33")]
        [TestCase("13в-33", "д. 13в, кв. 33")]
        [TestCase("13в 33", "д. 13в, кв. 33")]
        [TestCase(@"д 13в кв 330а", "д. 13в, кв. 330а")]
        [TestCase("13 330а", "д. 13, кв. 330а")]
        [TestCase("д 13 330а", "д. 13, кв. 330а")]
        [TestCase("д 13 2 330а", "д. 13, корп. 2, кв. 330а")]
        [TestCase("д 13 корп. 2 330а", "д. 13, корп. 2, кв. 330а")]
        [TestCase(@"13-1-1", "д. 13, корп. 1, кв. 1")]
        [TestCase(@"13 1 1", "д. 13, корп. 1, кв. 1")]
        [TestCase("д 13/12 корп. 2 1 330а", "д. 13/12, корп. 2, стр. 1, кв. 330а")]
        [TestCase("д 13/12 кв. 15", "д. 13/12, кв. 15")]
        [TestCase("13", "д. 13")]
        //[TestCase("д. 10, корп. Б кв А110", "д. 10, корп. Б, кв. А110")]
        [TestCase("д. 10, оф. 230", "д. 10, оф. 230")]
        [TestCase("д. 10, корп.1", "д. 10, корп. 1")]
        public void RegexTest(string input, string expected)
        {
            var acc = TakeToPieces(input).Single();

            string actual = string.Join(", ", acc.Select(i => string.Format("{0}. {1}", i.Key, i.Value)));

            Assert.AreEqual(expected, actual);
        }

        private static ICollection<IDictionary<string, string>> TakeToUnmarkedPeices(ICollection<string> pieces)
        {
            var result = new List<IDictionary<string, string>>();

            foreach (var mark in Marks)
            {

            }
        }

        private static ICollection<IDictionary<string, string>> TakeToPieces(string input)
        {
            var match = AddressSegmentsHelper.FullRegex.Match(" " + input);

            Assert.IsTrue(match.Success);

            Assert.AreEqual(2 + 1, match.Groups.Count);
            Assert.GreaterOrEqual(match.Groups["Mark"].Captures.Count, match.Groups["Number"].Captures.Count);

            var total = match.Groups["Number"].Captures.Count;
            var offset = total;

            int primaries = 2;

            IDictionary<string, string> acc = new Dictionary<string, string>();

            var skip = 0;

            foreach (var mark in Marks)
            {
                if (skip > 0)
                {
                    --skip;
                    continue;
                }
                if (offset == 0) break;

                var capturedMark =
                    match.Groups["Mark"].Captures[total - offset].Value.Replace("-", string.Empty)
                        .Replace(",", string.Empty)
                        .Trim();
                var capturedNumber = match.Groups["Number"].Captures[total - offset].Value;
                if (!string.IsNullOrEmpty(capturedMark))
                {
                    var tail = Marks.SkipWhile(i => i.Key != mark.Key);

                    var tailHead = tail.TakeWhile(i => !capturedMark.StartsWith((string)i.Value.label));
                    skip = tailHead.Count() - primaries;

                    if (skip > 0) continue;
                }
                else if (offset <= primaries && mark.Value.isPrimary != true) continue;

                acc.Add(mark.Value.label, capturedNumber);

                --offset;
                if (mark.Value.isPrimary == true) --primaries;
            }
            return new[] { acc };
        }

        private static readonly IDictionary<Mark, dynamic> Marks = new Dictionary<Mark, dynamic>()
        {
            {Mark.House, new{label="д", isPrimary=true}},
            {Mark.Corp, new{label="корп", isPrimary=false}},
            {Mark.Building, new{label="стр", isPrimary=false}},
            {Mark.Flat, new{label="кв", isPrimary=true}},
            {Mark.Office, new{label="оф", isPrimary=false}}
        };



    }

    public enum Mark : int { House = 1, Corp = 2, Building = 3, Flat = 4, Office = 5 }
}
