# dataexchanger
数据交换，负责数据获取、转换等工作，主要目的是统一数据异构、格式和分发，以及性能、事件广播处理，从而简化多服务间数据交互问题
1. 数据获取：支持sql、curl 两种主要方式获取数据
2. 支持数据缓存维护、数据修改事件广播
3. 数据转换：支持json数据结构转换、字段映射
4. 数据异构：支持多个接口数据集合，单输入接口分发到后端多服务
