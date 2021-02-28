using StackExchange.Redis;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace RedisTesting
{
    class Program
    {
        const string TestClassALabel = "TestClassA";
        const string TestClassBLabel = "TestClassB";
        static async Task Main(string[] args)
        {
            //await DoExpiryTests();
            await DoHashGetSetTests();
        }

        private static async Task DoHashGetSetTests()
        {
            var c = ConnectionMultiplexer.Connect("localhost:6379");
            var db = c.GetDatabase();

            Console.WriteLine(db.Ping());

            const string userAkey = "usera";
            const string userBkey = "userb";
            var userAitemA = new TestClassA { Id = 1, Name = "Charlie" };
            var userAitemAjson = JsonSerializer.Serialize(userAitemA);
            var userAitemB = new TestClassB { Question = "Huh?", Answer = "duh" };
            var userAitemBjson = JsonSerializer.Serialize(userAitemB);
            var userBitemA = new TestClassA { Id = 2, Name = "Mac" };
            var userBitemAjson = JsonSerializer.Serialize(userBitemA);
            var userBitemB = new TestClassB { Question = "News?", Answer = "Terrible news!" };
            var userBitemBjson = JsonSerializer.Serialize(userBitemB);

            var sec5 = TimeSpan.FromSeconds(5);
            var sec10 = TimeSpan.FromSeconds(10);
            var sec30 = TimeSpan.FromSeconds(30);
            var min30 = TimeSpan.FromMinutes(30);

            await SetHashValueWithExpiry(db, userAkey, TestClassALabel, userAitemAjson, sec30);
            await SetHashValueWithExpiry(db, userAkey, TestClassBLabel, userAitemBjson, sec30);
            await SetHashValueWithExpiry(db, userBkey, TestClassALabel, userBitemAjson, sec30);
            await SetHashValueWithExpiry(db, userBkey, TestClassBLabel, userBitemBjson, sec30);
            Console.WriteLine("set key with 30sec expiry");

            var userAall = await GetFullHashWithSlidingExpiry(db, userAkey, sec30);
            var userBall = await GetFullHashWithSlidingExpiry(db, userBkey, sec30);

            Console.WriteLine("will reset expiry in 10 seconds ...");
            await Task.Delay(sec10);
            var r = await GetHashFieldWithSlidingExpiry(db, userAkey, TestClassALabel, sec30);
            Console.WriteLine(r ?? "nil");

            Console.WriteLine("will reset expiry in 10 seconds ...");
            await Task.Delay(sec10);
            var r2 = await GetHashFieldWithSlidingExpiry(db, userBkey, TestClassALabel, sec30);
            Console.WriteLine(r2 ?? "nil");
            //db.KeyExpire(hiKey, sec30);
            //Console.WriteLine("refreshed key's 30sec expiry");
        }

        private static async Task DoExpiryTests()
        {
            var c = ConnectionMultiplexer.Connect("localhost:6379");
            var db = c.GetDatabase();

            Console.WriteLine(db.Ping());

            const string hiKey = "hiKey";
            const string hi = "hi";
            var sec5 = TimeSpan.FromSeconds(5);
            var sec10 = TimeSpan.FromSeconds(10);
            var sec30 = TimeSpan.FromSeconds(30);
            var min30 = TimeSpan.FromMinutes(30);

            await SetValueWithExpiry(db, hiKey, hi, sec30);
            Console.WriteLine("set key with 30sec expiry");

            Console.WriteLine("will reset expiry in 10 seconds ...");
            await Task.Delay(sec10);
            var r = await GetValueWithSlidingExpiry(db, hiKey, sec5);
            Console.WriteLine(r ?? "nil");

            Console.WriteLine("will reset expiry in 10 seconds ...");
            await Task.Delay(sec10);
            var r2 = await GetValueWithSlidingExpiry(db, hiKey, sec30);
            Console.WriteLine(r2 ?? "nil");
            //db.KeyExpire(hiKey, sec30);
            //Console.WriteLine("refreshed key's 30sec expiry");
        }

        private static async Task SetHashValueWithExpiry(IDatabase db, string hashKey, string field, string value, TimeSpan expiry)
        {
            await db.HashSetAsync(hashKey, field, value);
            await db.KeyExpireAsync(hashKey, expiry);
        }

        private static async Task<string> GetHashFieldWithSlidingExpiry(IDatabase db, string hashKey, string field, TimeSpan expiry)
        {
            await db.KeyExpireAsync(hashKey, expiry, CommandFlags.FireAndForget);
            return await db.HashGetAsync(hashKey, field);
        }

        private static async Task<TestClassAll> GetFullHashWithSlidingExpiry(IDatabase db, string hashKey, TimeSpan expiry)
        {
            await db.KeyExpireAsync(hashKey, expiry, CommandFlags.FireAndForget);
            var fullHash = await db.HashGetAllAsync(hashKey);
            return GetAllFromHash(fullHash);
        }

        private static TestClassAll GetAllFromHash(HashEntry[] fullHash)
        {
            var target = new TestClassAll();

            foreach (var item in fullHash)
            {
                if (item.Name.Equals(TestClassALabel)) target.A = JsonSerializer.Deserialize<TestClassA>(item.Value);
                if (item.Name.Equals(TestClassBLabel)) target.B = JsonSerializer.Deserialize<TestClassB>(item.Value);
            }

            return target;
        }

        private static async Task ClearHash(IDatabase db, string hashKey)
        {
            await db.KeyDeleteAsync(hashKey);
        }

        private static async Task SetValueWithExpiry(IDatabase db, string key, string value, TimeSpan expiry)
        {
            await db.StringSetAsync(key, value, expiry);
        }

        private static async Task<string> GetValueWithSlidingExpiry(IDatabase db, string key, TimeSpan expiry)
        {
            await db.KeyExpireAsync(key, expiry, CommandFlags.FireAndForget);
            return await db.StringGetAsync(key);
        }

        private class TestClassA
        {
            public long Id { get; set; }
            public string Name { get; set; }
        }

        private class TestClassB
        {
            public string Question { get; set; }
            public string Answer { get; set; }
        }

        private class TestClassAll
        {
            public TestClassA A { get; set; }
            public TestClassB B { get; set; }
        }
    }
}
