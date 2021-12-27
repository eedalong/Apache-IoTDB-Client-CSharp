<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
[English](./README.md) | [中文](./README_ZH.md)

# Apache IoTDB C#语言客户端

## 概览

本仓库是Apache IoTDB的C#语言客户端,与其他语言支持相同语义的用户接口。

Apache IoTDB website: https://iotdb.apache.org

Apache IoTDB Github: https://github.com/apache/iotdb

## 如何安装
### 从NuGet Package安装

我们为CSharp用户准备了NuGet包，用户可直接通过.NET CLI进行客户端安装，[NuGet包链接如下](https://www.nuget.org/packages/Apache.IoTDB/),命令行中运行如下命令即可完成安装
    
```sh
dotnet add package Apache.IoTDB
```

请注意，`Apache.IoTDB`这个包仅支持`.net 5.0`。 如果您使用的是`.net framework 4.x`，请参考[从`.net framework 4.x`开始](#从-net-framework-4x-开始)。

## 环境准备

    .NET SDK Version == 5.0 

## 如何使用 (快速上手)
用户可参考[使用样例](https://github.com/eedalong/Apache-IoTDB-Client-CSharp-UserCase)中的测试代码了解各个接口使用方式


## iotdb-client-csharp的开发者环境要求
    .NET SDK Version == 5.0
    ApacheThrift >= 0.14.1
    NLog >= 4.7.9


### 操作系统

* Linux、Macos或其他类unix系统
* Windows+bash(WSL、cygwin、Git Bash)

### 命令行工具

## 在 nuget.org 上发布你自己的客户端
你可以在这个[文档](./PUBLISH.md)中找到如何发布

## 从`.net framework 4.x`开始
为了适配`.net framework 4.x`，我们单独构建了一个Nuget包，包名是[`Apache.IoTDB.framework`](https://www.nuget.org/packages/Apache.IoTDB.framework/)。

您可以使用PM、.NET CLI等工作来安装它。以.NET CLI为例：

```sh
dotnet add package Apache.IoTDB.framework --version 0.12.1.2
```