#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["samples/Apache.IoTDB.Samples/Apache.IoTDB.Samples.csproj", "samples/Apache.IoTDB.Samples/"]
COPY ["src/Apache.IoTDB/Apache.IoTDB.csproj", "src/Apache.IoTDB/"]
RUN dotnet restore "samples/Apache.IoTDB.Samples/Apache.IoTDB.Samples.csproj"
COPY . .
WORKDIR "/src/samples/Apache.IoTDB.Samples"
RUN dotnet build "Apache.IoTDB.Samples.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Apache.IoTDB.Samples.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Apache.IoTDB.Samples.dll"]