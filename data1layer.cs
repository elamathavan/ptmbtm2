using System;
using System.Data;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using Newtonsoft.Json.Linq;

namespace FltPTMBTM
{
    public static class DataLayer
    {
        private static readonly string tokenUrl = "https://nsk-dotrezapi-nonprod-3scale-apicast-production.apps.ocpnonprodcl01.goindigo.in/api/nsk/v2/token";
        private static readonly string manifestUrl = "https://dotrezapi.test.6e.navitaire.com/api/nsk/v2/manifest";
        private static readonly string manifestLegUrl = "https://dotrezapi.test.6e.navitaire.com/api/nsk/v2/manifest/";

        private static readonly string userKey = "b0038a28b62f1e7c5bab9e869757e7b6";

        // credentials for token generation
        private static readonly string tokenPayload = @"
        {
          ""credentials"": {
            ""username"": ""OPSFLIFOApp"",
            ""password"": ""Indigo$789"",
            ""domain"": ""EXT"",
            ""ChannelType"":""Direct""
          }
        }";

        private static string Token = "";

        // ======================================================
        // Get Token
        // ======================================================
        public static string GetToken()
        {
            if (!string.IsNullOrEmpty(Token))
                return Token;

            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("user_key", userKey);

                var content = new StringContent(tokenPayload, Encoding.UTF8, "application/json");
                var response = client.PostAsync(tokenUrl, content).Result;

                var json = response.Content.ReadAsStringAsync().Result;
                var obj = JObject.Parse(json);

                Token = obj["accessToken"].ToString();
                return Token;
            }
        }

        // ======================================================
        // 1️⃣  Replaces SP: GetFlightDataDetails → API: /manifest
        // ======================================================
        public static DataTable GetFlightDataDetails()
        {
            string token = GetToken();

            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", token);

                string url = manifestUrl +
                    "?origin=DEL&destination=BLR&carrierCode=6E&beginDate=2025-02-05&identifier=A&flightType=0";

                var result = client.GetAsync(url).Result;
                string json = result.Content.ReadAsStringAsync().Result;
                var data = JObject.Parse(json);

                // create DataTable that matches SP output
                DataTable dt = new DataTable();
                dt.Columns.Add("AIRLINE");
                dt.Columns.Add("FLTNBR");
                dt.Columns.Add("DEP");

                string airline = "6E";
                string flt = data["flightNumber"]?.ToString();
                string dep = data["origin"]?.ToString();

                dt.Rows.Add(airline, flt, dep);
                return dt;
            }
        }

        // ======================================================
        // 2️⃣  Replaces SP: PTMBTM Insert → API: /manifest/{legKey}
        // ======================================================
        public static void GetAndInsertPTMBTMData(DateTime runDate, string carrier, string flightNo, string opSuffix, string departure)
        {
            string token = GetToken();

            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", token);

                // STEP 1: Call manifest API again to extract legKey
                string url = manifestUrl +
                    "?origin=" + departure +
                    "&destination=BLR&carrierCode=" + carrier +
                    "&beginDate=" + runDate.ToString("yyyy-MM-dd") +
                    "&identifier=A&flightType=0";

                var res1 = client.GetAsync(url).Result;
                var json1 = JObject.Parse(res1.Content.ReadAsStringAsync().Result);

                string legKey = json1["legs"]?[0]?["legKey"]?.ToString();

                if (string.IsNullOrEmpty(legKey))
                    throw new Exception("legKey missing in manifest API response.");

                // STEP 2: Get the BTM/PTM info for that legKey
                var res2 = client.GetAsync(manifestLegUrl + legKey).Result;
                var json2 = JObject.Parse(res2.Content.ReadAsStringAsync().Result);

                // Here you can insert into DB or process data as needed
                Console.WriteLine("Received passenger count: " + json2["passengers"]?.Count());
                Console.WriteLine("Received BTM count: " + json2["btm"]?.Count());
            }
        }
    }
}
