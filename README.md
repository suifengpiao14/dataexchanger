# datacenter
数据中心，负责数据获取、转换等工作
1. 数据获取：支持sql、curl 两种主要方式获取数据，支持数据缓存维护、数据修改事件广播
2. 数据转换：支持json数据结构转换、字段映射

基于https://github.com/d5/tengo 融合 template/txt 库