using SalesforceSync.LoadGenerator;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<LoadGeneratorOptions>(builder.Configuration.GetSection(LoadGeneratorOptions.Section));
builder.AddKafkaProducer<string, string>("kafka");
builder.Services.AddHostedService<AccountEventProducer>();

builder.Build().Run();