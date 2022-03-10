#nullable enable

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace SqsPriorityQueue
{
    public static class ConsoleApp
    {
        public static Task Init<T>(string[] args, Action<HostBuilderContext, IServiceCollection> configureDelegate) 
            where T : class, IHostedService
            => 
            CreateHostBuilder<T>(args, configureDelegate).Build().RunAsync();

        private static IHostBuilder CreateHostBuilder<T>(string[] args, Action<HostBuilderContext, IServiceCollection> configureDelegate)
            where T : class, IHostedService
            =>
            Host.CreateDefaultBuilder(args).ConfigureServices((c,s) => ConfigureServicesInternal<T>(c, s, configureDelegate));

        private static void ConfigureServicesInternal<T>(HostBuilderContext context, IServiceCollection services, Action<HostBuilderContext, IServiceCollection> configureDelegate)
            where T : class, IHostedService
        {
            services.AddHostedService<T>();
            services.AddLogging();

            configureDelegate(context, services);
        }

        /// <summary>
        /// Makes a section of the appsettings.json file accessible as a strongly-typed class
        /// resolvable via DI.
        /// </summary>
        /// <typeparam name="TSettingsSection">A class representing settings in a section of the appsettings.json</typeparam>
        /// <param name="services"></param>
        /// <param name="config"></param>
        public static void RegisterAppSettingsSection<TSettingsSection>(this IServiceCollection services, IConfiguration config) 
            where TSettingsSection : class
        {
            services.Configure<TSettingsSection>(config.GetSection(typeof(TSettingsSection).Name));
            services.AddTransient(ioc => ioc.GetRequiredService<IOptions<TSettingsSection>>().Value);
        }
    }
}
