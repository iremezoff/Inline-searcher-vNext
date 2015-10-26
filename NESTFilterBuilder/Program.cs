using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.SelfHost;
using Elasticsearch.Net;
using Microsoft.Owin;
using Microsoft.Owin.Cors;
using Microsoft.Owin.Hosting;
using Nest;
using NESTFilterBuilder;
using Owin;

[assembly: OwinStartup(typeof(Startup))]

namespace NESTFilterBuilder
{
    class Program
    {
        static void Main(string[] args)
        {
            string baseUri = "http://localhost:34567";

            Console.WriteLine("Starting web Server...");
            WebApp.Start<Startup>(baseUri);
            Console.WriteLine("Server running at {0} - press Enter to quit. ", baseUri);
            Console.ReadLine();


            var config = new ConnectionSettings(new Uri("http://localhost:9200"), "fias2");


            var client = new ElasticClient(config);

            var result =
                client.Search<IndexBulkItem>(
                    d =>
                        d.Query(
                            q =>
                                q.Filtered(
                                    fd =>
                                        fd.Filter(
                                            f =>
                                                f.And(and => and.Term("idExpanded.region", Guid.Parse("D66E5325-3A25-4D29-BA86-4CA351D9704B"))
                                                //,and => and.Term(i => i.ParentLevel, 1)
                                                    )
                                            )
                                            .Query(
                                                qq =>
                                                    qq.QueryString(
                                                        qs =>
                                                            qs.OnFields(new[] { "district", "city", "cityArea", "locality", "street" })
                                                                .Query("Лени*")))))
                            .From(0)
                            .Size(100)
                            .Sort(f => f.NestedPath(i => i.SortOrder)));




            var data = result.Hits.Select(h => h.Source);

            var elapsed = result.ElapsedMilliseconds;

        }
    }


    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            //var listener = (HttpListener)app.Properties["System.Net.HttpListener"];
            //listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication;

            var config = new HttpConfiguration();

            config.Routes.MapHttpRoute(
                "DefaultApi",
                "{controller}/{id}",
                new { id = RouteParameter.Optional });

            app.UseCors(CorsOptions.AllowAll);
            app.UseWebApi(config);
        }
    }

    public class DataController : ApiController
    {
        private ElasticClient _client;
        private Dictionary<int, IndexItemMeta> _fieldGetter;

        public DataController()
        {
            var config = new ConnectionSettings(new Uri("http://localhost:9200"), "fias2");


            _client = new ElasticClient(config);

            _fieldGetter = new List<IndexItemMeta>
            {
                new IndexItemMeta()
                {
                    Level = 1,
                    FieldName = "region",
                    GuidGetter = i => i.IdExpanded.Region ?? Guid.Empty,
                    NameGetter = i => i.Region
                },
                new IndexItemMeta()
                {
                    Level = 2,
                    FieldName = "ao",
                    GuidGetter = i => i.IdExpanded.Ao ?? Guid.Empty,
                    NameGetter = i => i.Ao
                },
                new IndexItemMeta()
                {
                    Level = 3,
                    FieldName = "district",
                    GuidGetter = i => i.IdExpanded.District ?? Guid.Empty,
                    NameGetter = i => i.District
                },
                new IndexItemMeta()
                {
                    Level = 4,
                    FieldName = "city",
                    GuidGetter = i => i.IdExpanded.City ?? Guid.Empty,
                    NameGetter = i => i.City
                },
                new IndexItemMeta()
                {
                    Level = 5,
                    FieldName = "cityArea",
                    GuidGetter = i => i.IdExpanded.CityArea ?? Guid.Empty,
                    NameGetter = i => i.CityArea
                },
                new IndexItemMeta()
                {
                    Level = 6,
                    FieldName = "locality",
                    GuidGetter = i => i.IdExpanded.Locality ?? Guid.Empty,
                    NameGetter = i => i.Locality
                },
                new IndexItemMeta()
                {
                    Level = 7,
                    FieldName = "street",
                    GuidGetter = i => i.IdExpanded.Street ?? Guid.Empty,
                    NameGetter = i => i.Street
                },
                new IndexItemMeta()
                {
                    Level = 90,
                    FieldName = "addArea",
                    GuidGetter = i => i.IdExpanded.AddArea ?? Guid.Empty,
                    NameGetter = i => i.AddArea
                },
                new IndexItemMeta()
                {
                    Level = 91,
                    FieldName = "addStreet",
                    GuidGetter = i => i.IdExpanded.AddStreet ?? Guid.Empty,
                    NameGetter = i => i.AddStreet
                }

            }.ToDictionary(k => k.Level, v => v);
        }

        public IHttpActionResult Get(Guid? currentItem = null, string lastItemRaw = null, int level = 0, string queryString = null)
        {
            if (currentItem != null && level == 0)
            {
                var query = _client.Search<IndexBulkItem>(
                       d => d.Query(qd => qd.Filtered(fqd => fqd.Filter(fd => fd.Term("_id", currentItem)))));
                if (query.Total == 0)
                {
                    throw new Exception("Не найден");
                }
                if (query.Total > 1)
                {
                    throw new Exception("Нарушена уникальность");
                }

                level = query.Documents.Single().Level;
            }

            var searchRequest = new SearchRequest
            {
                From = 0,
                Size = 100,
                Sort = new List<KeyValuePair<PropertyPathMarker, ISort>>() { new KeyValuePair<PropertyPathMarker, ISort>(new PropertyPathMarker() { Name = "sortOrder" }, new Sort()) }
            };

            searchRequest.Filter = new FilterContainer();

            if (currentItem != null)
            {
                //fd.Filter(f => f.Term("idExpanded." + _fieldGetter[level].FieldName, currentItem));
                searchRequest.Filter.Term = new TermFilter()
                {
                    Field = new PropertyPathMarker() { Name = "idExpanded." + _fieldGetter[level].FieldName },
                    Value = currentItem
                };
            }

            Func<IndexBulkItem, string> resultExtractor = null;
            Func<IndexBulkItem, IEnumerable<SearchResult>> hierarchyExtractor = null;

            bool needSearch = true;
            IEnumerable<string> segments = new List<string>();

            if (string.IsNullOrEmpty(queryString))
            {
                if (currentItem == null)
                {
                    searchRequest.Filter = new FilterContainer();
                    searchRequest.Filter.Missing = new MissingFilter
                    {
                        Field = new PropertyPathMarker() { Name = "parentGuid" }
                    };
                }
                else
                    searchRequest.Filter.Term = new TermFilter()
                    {
                        Field = new PropertyPathMarker() { Name = "parentGuid" },
                        Value = currentItem
                    };
                resultExtractor = i => i.Title;
                hierarchyExtractor = i => new[] { new SearchResult() { Label = i.Title, Level = i.Level, Value = i.Guid } };
            }
            else
            {
                if (level >= 7)
                {
                    //int index = AddressSegmentsHelper.Segments.TakeWhile(segment => !segment.Value.Regex.IsMatch(queryString)).Count();
                    int index = AddressSegmentsHelper.Segments.TakeWhile(segment => !segment.Value.Regex.IsMatch(lastItemRaw)).Count();

                    if (index < AddressSegmentsHelper.Segments.Count)
                    {
                        needSearch = false;

                        var extractedSegments = AddressSegmentsHelper.Extract(lastItemRaw + " " + queryString).Where(
                            s => !string.IsNullOrEmpty(s.Value) || !string.IsNullOrWhiteSpace(s.Value));

                        if (extractedSegments.Any(s => s.Key.Equals(lastItemRaw)))
                        {
                            segments = new List<string>();
                        }

                        segments = extractedSegments.Skip(1).Select(s => string.Format("{0} {1}", s.Key, s.Value));
                    }
                    else
                    {
                        int queryIndex = AddressSegmentsHelper.Segments.TakeWhile(segment => !segment.Value.Regex.IsMatch(queryString)).Count();

                        if (queryIndex < AddressSegmentsHelper.Segments.Count)
                        {
                            needSearch = false;

                            var extractedSegments = AddressSegmentsHelper.Extract(queryString).Where(
                                s => !string.IsNullOrEmpty(s.Value) || !string.IsNullOrWhiteSpace(s.Value));

                            if (extractedSegments.Any(s => s.Key.Equals(lastItemRaw)))
                            {
                                segments = new List<string>();
                            }

                            segments = extractedSegments.Select(s => string.Format("{0} {1}", s.Key, s.Value));
                        }
                    }
                }

                if (needSearch)
                {
                    var fields = _fieldGetter.Where(f => f.Key > level).Select(f => f.Value.FieldName);

                    // формирование нечеткого запроса
                    searchRequest.Query = new QueryContainer();
                    searchRequest.Query.QueryString = new QueryStringQuery();
                    searchRequest.Query.QueryString.Fields =
                        fields.ToArray().Select(i => new PropertyPathMarker() { Name = i });
                    searchRequest.Query.QueryString.Query = string.Format("{0}*", queryString);

                    resultExtractor = i => i.FullAddress;
                    hierarchyExtractor =
                        i =>
                            _fieldGetter.Where(f => f.Key > level && f.Value.GuidGetter(i) != Guid.Empty)
                                .Select(f => new SearchResult()
                                {
                                    Label = f.Value.NameGetter(i),
                                    Value = f.Value.GuidGetter(i),
                                    Level = f.Key
                                });
                }
            }

            IEnumerable<SearchResult> response = null;

            if (needSearch)
            {
                var result = _client.Search<IndexBulkItem>(searchRequest);

                response = result.Documents.Select(
                    d =>
                        new SearchResult()
                        {
                            Label = resultExtractor(d),
                            Level = d.Level,
                            Value = d.Guid,
                            Hierarchy = hierarchyExtractor(d).ToArray()
                        });

            }
            else if (segments.Any())
            {
                response = new[]
                    {
                        new SearchResult()
                        {
                            Label = string.Join(", ", segments),
                            Value = currentItem.Value,
                            Level = level,
                            Hierarchy = segments.Select(
                                s =>
                                    new SearchResult()
                                    {
                                        Label = s,
                                        Value = currentItem.Value,
                                        Level = level
                                    }).ToArray()
                        }
                    };
            }

            if (response != null)
                return Ok(response);
            return NotFound();

            //return
            //    Ok(
            //        result.Documents.Select(
            //            d => new SearchResult
            //            {
            //                Label =
            //                    (string.IsNullOrEmpty(queryString) ? d.Title : d.FullAddress) +
            //                    MakeFullAddress(lastItemRaw.Select(
            //            i => AddressSegmentsHelper.Extract(i).Single(r => !string.IsNullOrEmpty(r.Value)))
            //            .Union(segments.Where(s => !string.IsNullOrEmpty(s.Value)))
            //            .ToDictionary(i => i.Key, i => i.Value)),
            //                Value = d.Guid,

            //                Level = d.Level,
            //                Hierarchy =
            //                    _fieldGetter.Where(i => i.Key > level)
            //                        .Select(
            //                            i =>
            //                                new SearchResult()
            //                                {
            //                                    Label = i.Value.NameGetter(d),
            //                                    Value = i.Value.GuidGetter(d),
            //                                    Level = i.Key
            //                                }

            //                                ).Where(i => i.Value != Guid.Empty)
            //                        .ToList()
            //                        .Union(
            //                            segments.Where(kv => !string.IsNullOrEmpty(kv.Value))
            //                                .Select(
            //                                    kv =>
            //                                        new SearchResult
            //                                        {
            //                                            Label = string.Format("{0} {1}", kv.Key, kv.Value),
            //                                            Value =
            //                                                        _fieldGetter.Where(
            //                                                            i =>
            //                                                                i.Key >= level &&
            //                                                                i.Value.GuidGetter(d) != Guid.Empty)
            //                                                            .Max()
            //                                                            .Value.GuidGetter(d),
            //                                            Level = _fieldGetter.Where(
            //                                                        i =>
            //                                                            i.Key >= level &&
            //                                                            i.Value.GuidGetter(d) != Guid.Empty)
            //                                                        .Max().Value.Level
            //                                        })).ToArray()
            //            }
            //            ));
        }

        private static string MakeFullAddress(IDictionary<string, string> extractedSegments)
        {
            var tailBuilder = new StringBuilder();
            foreach (var segment in extractedSegments)
            {
                if (!string.IsNullOrEmpty(segment.Value) || !string.IsNullOrWhiteSpace(segment.Value))
                {
                    tailBuilder.AppendFormat(", {0} {1}", segment.Key, segment.Value);
                }
            }
            return tailBuilder.ToString();
        }
    }

    class IndexItemMeta : IComparable<IndexItemMeta>
    {
        public int Level { get; set; }
        public string FieldName { get; set; }
        public Func<IndexBulkItem, Guid> GuidGetter { get; set; }
        public Func<IndexBulkItem, string> NameGetter { get; set; }
        public int CompareTo(IndexItemMeta other)
        {
            return Level.CompareTo(other.Level);
        }
    }

    [DebuggerDisplay("{Level} - {Label}")]
    public class SearchResult
    {
        public Guid Value { get; set; }
        public SearchResult[] Hierarchy { get; set; }
        public string Label { get; set; }
        public int Level { get; set; }
    }

    //    {
    //  "query": {
    //    "filtered": {
    //      "filter": {
    //        "and": [
    //          {
    //            "term": {
    //              "region.raw": "Ханты-Мансийский Автономный округ - Югра"
    //            }
    //          },
    //          {
    //            "term": {
    //              "parentLevel": 1
    //            }
    //          }
    //        ]
    //      }
    //    }
    //  },
    //  "from": 0,
    //  "size": 100,
    //  "sort": [
    //    {
    //      "sortOrder": {
    //        "order": "asc"
    //      }
    //    }
    //  ],
    //  "facets": {}
    //}


    [ElasticType(Name = "indexbulkitem", IdProperty = "Guid")]
    public class IndexBulkItem
    {
        [ElasticProperty]
        public string FullAddress { get; set; }
        [ElasticProperty(Index = FieldIndexOption.No)]
        public string Title { get; set; }
        [ElasticProperty]
        public string Region { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string Ao { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string District { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string City { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string CityArea { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string Locality { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string Street { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string AddArea { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed)]
        public string AddStreet { get; set; }
        public Guid Guid { get; set; }

        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid ParentGuid { get; set; }
        [ElasticProperty(Index = FieldIndexOption.No)]
        public string CodeKLADR { get; set; }

        public int Level { get; set; }
        public int ParentLevel { get; set; }
        public int SortOrder { get; set; }
        [ElasticProperty(Index = FieldIndexOption.No)]
        public string CodeFias { get; set; }

        public IdExpanded IdExpanded { get; set; }
    }

    public class IdExpanded
    {
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? Region { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? Ao { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? District { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? City { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? CityArea { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? Locality { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? Street { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? AddArea { get; set; }
        [ElasticProperty(Index = FieldIndexOption.NotAnalyzed, Type = FieldType.String)]
        public Guid? AddStreet { get; set; }
    }

    public static class AddressSegmentsHelper
    {
        public const string HouseSign = "д.";
        public const string CorpsSign = "корп.";
        public const string BuildingHouseSign = "стр.";
        public const string RoomSign = "кв.";
        public const string OfficeSign = "оф.";

        private static readonly Regex UniferedSegmentRegex = new Regex(@"(?:\s+(?:((\d+\w*))[\s|\-|\,|$]*)+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        private const string DeniedSymbolsRegEx = @"[\[\]\{\}?|%^<>]";

        public static readonly IDictionary<string, SegmentMetadata> Segments = new Dictionary<string, SegmentMetadata>
        {
            {HouseSign, new SegmentMetadata {Regex =new Regex(@"\s*(?:д[ом\.]*)\s*((?:\d\w+[[/|-]\d+]*)|(?:\d+\w{0,6}))[$]?",RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline),IsPrimary = true}},
            {CorpsSign, new SegmentMetadata {Regex =new Regex(@"\s*(?:корп[ус\.]*)\s*((?:\d+[/|-]\d+)|(?:\d+\w{0,3})|(\w?\d*))[$]?",RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline),IsPrimary = false}},
            {BuildingHouseSign, new SegmentMetadata {Regex =new Regex(@"\s*(?:с[троение\.]*)\s*((?:\d+[/|-]\d+)|(?:\d+\w{0,3}))[$]?",RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline),IsPrimary = false}},
            {RoomSign, new SegmentMetadata {Regex =new Regex(@"\s*(?:кв[артира\.]*)\s*((?:\d\w+[[/|-]\d+]*)|(?:\d+\w{0,6})|(\w?\d*))[$]?",RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline),IsPrimary = true}},
            {OfficeSign, new SegmentMetadata {Regex =new Regex(@"\s*(?:оф(?:[ис\.])*)\s*((?:\d\w+[[/|-]\d+]*)|(?:\d+\w{0,6}))[$]?",RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled),IsPrimary = false}}
        };

        public  static readonly Regex FullRegex = new Regex(@"(?:[\s,-]*(?'Mark'(?:д[ом\.\s]*)|(?:корп[ус\.\s]*)|(?:с[троение\.\s]*)|(?:кв[артира\.\s]*)|(?:оф(?:[ис\.\s]*))|[\s-,])+(?'Number'(?:\d\w*[[/]\d+]*)|(?:\d+\w{0,6})))*$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public class SegmentMetadata
        {
            public Regex Regex { get; set; }
            public bool IsPrimary { get; set; }
        }

        public static IDictionary<string, string> Extract(string address)
        {
            var inferedSegments = ExtractSegments(address);
            var uninferedSegments = ExtractUninferedSegments(address);
            var segments = Merge(inferedSegments, uninferedSegments);

            return segments;
        }

        static string ExtractSegment(MatchCollection matches)
        {
            if (matches.Count < 1)
                return string.Empty;

            if (matches[0].Groups.Count < 1)
                return string.Empty;

            return matches[0].Groups[1].Value;

        }

        static IDictionary<string, string> ExtractSegments(string address)
        {
            return Segments.ToDictionary(k => k.Key, v => ExtractSegment(v.Value.Regex.Matches(address)));

            //Regex expression = new Regex(HomePartRegex, RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
            //return ExtractSegment(HomePartRegex.Matches(address));
        }

        public static string Normalaize(string term)
        {
            //var regex3 = new Regex(UniferedSegmentRegex, RegexOptions.IgnoreCase | RegexOptions.Multiline);
            //if (UniferedSegmentRegex.IsMatch(term))
            //term = UniferedSegmentRegex.Replace(term, string.Empty) + ","; // запятая ещё пригодится
            term = NormalaizeOnExplicitedSegments(term);//  FlatPartRegex.Replace(HomePartRegex.Replace(UniferedSegmentRegex.Replace(address, string.Empty), string.Empty), string.Empty);
            term = NormalaizeOnImplicitedSegments(term);
            var deniedSymbolsRegex = new Regex(DeniedSymbolsRegEx);
            term = deniedSymbolsRegex.Replace(term, "");
            return term;
        }

        private static string NormalaizeOnExplicitedSegments(string term)
        {
            //var regex3 = new Regex(UniferedSegmentRegex, RegexOptions.IgnoreCase | RegexOptions.Multiline);
            term = Segments.Aggregate(term, (acc, kv) => kv.Value.Regex.Replace(acc, string.Empty));//  FlatPartRegex.Replace(HomePartRegex.Replace(UniferedSegmentRegex.Replace(address, string.Empty), string.Empty), string.Empty);
            var deniedSymbolsRegex = new Regex(DeniedSymbolsRegEx);
            term = deniedSymbolsRegex.Replace(term, "");
            return term;
        }

        private static string NormalaizeOnImplicitedSegments(string term)
        {
            //var regex3 = new Regex(UniferedSegmentRegex, RegexOptions.IgnoreCase | RegexOptions.Multiline);
            term = UniferedSegmentRegex.Replace(term, string.Empty);// Aggregate(term, (acc, kv) => kv.Value.Regex.Replace(acc, string.Empty));//  FlatPartRegex.Replace(HomePartRegex.Replace(UniferedSegmentRegex.Replace(address, string.Empty), string.Empty), string.Empty);
            //var deniedSymbolsRegex = new Regex(DeniedSymbolsRegEx);
            //term = deniedSymbolsRegex.Replace(term, "");
            return term;
        }

        private static string[] ExtractUninferedSegments(string term)
        {
            term = NormalaizeOnExplicitedSegments(term);
            //var regex3 = new Regex(UniferedSegmentRegex, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            var match = UniferedSegmentRegex.Match(term);
            if (match.Success)
            {
                return match.Groups[1].Captures.OfType<Capture>().Select(i => i.Value).ToArray();
            }
            string result = string.Empty;
            //if (match.Index > 1)
            //{
            //    result = address.Substring(match.Index);
            //}
            return new string[0];
        }



        private static IDictionary<string, string> Merge(IDictionary<string, string> inferedSegments, string[] uninferedSegments)
        {
            int remained = uninferedSegments.Count();
            if (remained == 0)
                return inferedSegments;

            int index = 0;
            int primaries = Segments.Count(i => i.Value.IsPrimary);

            return inferedSegments.ToDictionary(k => k.Key, i =>
            {
                if (Segments[i.Key].IsPrimary)
                {
                    primaries--;
                }
                if (string.IsNullOrEmpty(i.Value) && (Segments[i.Key].IsPrimary || primaries < remained) && index < uninferedSegments.Length)
                {
                    remained--;
                    return uninferedSegments[index++];
                }
                //if (Segments[i.Key].IsPrimary && !string.IsNullOrEmpty(i.Value))
                //{
                //    return i.Value;
                //}

                return i.Value;
            });
        }

        public static string GetFormatedSegment(IDictionary<string, string> segments, string segmentKey)
        {
            if (segmentKey == null)
            {
                throw new ArgumentNullException("segmentKey");
            }

            string segmentValue;

            if (segments.TryGetValue(segmentKey, out segmentValue) && !string.IsNullOrEmpty(segmentValue))
            {
                return segmentValue;
            }
            return null;
        }
    }
}
