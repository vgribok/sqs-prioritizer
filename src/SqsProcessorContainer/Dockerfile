#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

# To build, execute this command from the directory of the Dockerfile:
# docker build -f Dockerfile .. -t sqsprocessor:latest

# To run locally using your current aws cli credentials, execute following lines in PowerShell:
# $access_key=aws configure get aws_access_key_id
# $secret_key=aws configure get aws_secret_access_key
# $sqs_arns="<YOUR COMMA-DELIMITED QUEUE ARN STARTING WITH HIGHEST PRIORITY>"
# docker run -it -e AppSettings__QeueueArns=$sqs_arns -e AWS_ACCESS_KEY_ID=$access_key -e AWS_SECRET_ACCESS_KEY=$secret_key -e Logging__LogLevel__Default=Trace --name sqs-processor sqsprocessor:latest

# To stop container running locally, run following lines:
# docker stop sqs-processor (or Ctrl+C and then "docker rm sqs-processor")
# docker rm sqs-processor

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["SqsProcessorContainer/SqsProcessorContainer.csproj", "SqsProcessorContainer/"]
RUN dotnet restore SqsProcessorContainer/SqsProcessorContainer.csproj
COPY . .
WORKDIR /src/SqsProcessorContainer
RUN dotnet build SqsProcessorContainer.csproj -c Release -o /app/build

FROM build AS publish
RUN dotnet publish SqsProcessorContainer.csproj -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SqsProcessorContainer.dll"]