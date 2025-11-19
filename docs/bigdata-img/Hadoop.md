## HDFS 写入流程

```
@startuml
title HDFS 写入流程

actor Client
participant NameNode
participant DataNodeA
participant DataNodeB
participant DataNodeC

== 请求上传 ==
Client -> NameNode: RPC 请求上传文件
note right: 检查权限、重名

alt 权限不足或文件已存在
    NameNode --> Client: 报错
else 权限通过且文件不存在
    NameNode --> Client: 允许上传

    == 切分文件并请求 block 位置 ==
    Client -> NameNode: 请求第一个 block 上传的 DataNode 列表
    note right: 根据拓扑、机架感知、副本机制
    NameNode --> Client: 返回 DataNodeA、B、C 地址

    == 建立 pipeline ==
    Client -> DataNodeA: 建立 pipeline（RPC）
    DataNodeA -> DataNodeB: 建立 pipeline
    DataNodeB -> DataNodeC: 建立 pipeline
    DataNodeC --> DataNodeB: 确认
    DataNodeB --> DataNodeA: 确认
    DataNodeA --> Client: pipeline 建立完成

    == 传输 block 数据 ==
    loop 每个 packet（64KB）
        Client -> DataNodeA: 发送 packet
        DataNodeA -> DataNodeB: 转发 packet
        DataNodeB -> DataNodeC: 转发 packet
        DataNodeC --> DataNodeB: ack
        DataNodeB --> DataNodeA: ack
        DataNodeA --> Client: ack
    end

    == 请求下一个 block ==
    Client -> NameNode: 请求下一个 block 的 DataNode 列表
    NameNode --> Client: 返回新的 DataNode 列表
    note right: 重复上述流程直到所有 block 上传完成
end

@enduml
```

![](https://www.plantuml.com/plantuml/png/ZPHTJzDG6CVlpwSuNu0xumN93Ap4c9WGev-WZXDjA6i3n_jEWNNCXX7cT30OH9OGdTrySHJOt8UnvxIxuYluRAUM7IZL2qZsvFT_NlvFdpEtHjLL-cf5HrIZEi6F5Xu-msoppZPQ_6SXE2qZfEQeiOeLNIDvYaoWjPncgdcA5zKLicWi4Icue59r79nFZ6OJemf2cGmEEsVywslVs_BxHpYJGQ8YddaG5vh5Jvye4iThHV-oX_869NXLU_6IpcB-qUBD5ZzuCwfNVbk5KT5X5GUcq2aMGTzpmjvtRjU4c9rzOry-iCOfmXZ7fV0Cr1KTGCgjrgXQHqHV8r6EaRKN3ek-LnKv8EM_Sh2DGkXweciqmM1aPXUPlHdrSTuJa-7dkf5Rnlx02GOkK1CuoGo11-ssxrc-zodIYHB1tdfiBvHv7nwBtYIdZiwvu_BoBY_jW5EySS4FUtoz6JHRy9ELEhpHXY2pUtUF5GwhRF_mUkiWpCAVWbcZomwiwqalBuBFPMng9j6rFBazLfnXzYPxrRTXxLVzqaGHSzEYx2sHn6QdMIMTLV2Cd3WuTaFtv6O-gUCaPbwoAPvRb6RkMsvNOdDWN-6ltMX__5sNE_7UTSCmCUziZ_ThghbbGi6E-_SUP__wSRUBspiZgn2919fYNpXeWq2YKmnCeDCi14n0AHucGfA9CK7oIyAwgKUZBDr0oj4a2nBF9VsRvxNk_npP-FqvSU0ems5NlAZ1_WzcTtd9uesITBFHLp4URmx-mQFy1m00)

## HDFS 文件读取流程

```
@startuml
title HDFS 文件读取流程

actor Client
participant NameNode
participant "DataNode1" as DN1
participant "DataNode2" as DN2
participant "DataNode3" as DN3

== 请求 block 位置 ==
Client -> NameNode: RPC 请求文件 block 位置
note right: 检查权限、文件是否存在

alt 权限不足或文件不存在
    NameNode --> Client: 报错
else 权限通过且文件存在
    NameNode --> Client: 返回 block 列表及对应 DataNode 地址
    note right: 每个 block 返回多个副本地址\n按距离排序（拓扑 + 心跳 STALE 置后）

    loop 对每个 block
        == 选择 DataNode 并读取 ==
        Client -> DN1: 建立 Socket Stream（FSDataInputStream）
        note right: 选取排序最靠前的 DataNode\n本地 DataNode 则短路读取
        Client <-- DN1: 读取 block 数据
        note right: 并行读取，非顺序阻塞\n重复调用 read() 直到 block 读完

        == checksum 校验 ==
        alt 校验失败或读取异常
            Client -> NameNode: 报告 DataNode 读取失败
            Client -> DN2: 尝试从下一个副本读取
        end
    end

    == 继续获取下一批 block ==
    alt 文件未读完
        Client -> NameNode: 请求下一批 block 列表
        NameNode --> Client: 返回新的 block 列表及 DataNode 地址
        note right: 重复上述读取流程
    end

    == 合并文件 ==
    note over Client: 所有 block 读取完成后\n在客户端合并成完整文件
end

@enduml
```

![](https://www.plantuml.com/plantuml/png/XLJDJY9P5DxVfpXnDPC92tLdXeaJ7JEJJCnac2MR6goCH2W8bBCkjHEh-BSRQNz0fLjjMbiAeoqbXU5XkkwjgfMlq9S-LmH2xBk0rFdvplUTUywTJwjIIbsFnuWQLMCo_Bwu509MsNRiEy-qQR72Fc-uZImXKaHDf60X5fKLbIHvJZGIJKgA2ijIN5vEhCWZngb5IPNwrkafaDAmk3mzsJqZt3EJtRF2FKj8C0YUsMRNc_1lB15P0-SXxpuq8HWao0a2lmoepC7VVos8SDGoaaIKX2f3AlhVgZe7xBt6ZizORSlVBttHDZ2SxPcqT4wlzcYrmQN7LC00nyfxTpTChs0O_nGn032e3W5E1Kbn-CoPNzudSYmj2mnVE_1wsuvLHepl0NYzCZqy4lof_jQhDsWnGyrxsYd3Kv-0Lbkqfdr36H5d5Xth0fC54Zqzu2PgcApw2RF22iiPNhlcdjkiy9fsYezTdMNVC6C7VWBQs_BQDn3wvzS_VmFUEregF7ODGe2VM2AH1Cvak0e1SVXz-Ph1iX-7IDwBcUBEGTppxV4XcGDgTzpBB8GubAn2I4t9KfppMGhrKVvGaklgayr0Y375l2OlW3fOLVDh9zJ8kmUl1Yp22WeVegNlkyTNNjj4SkFCVWu4a1gwHJFPRelbcnCfz6NMSnZzsCtvjIE_tk5q_3sRre_2Yh-TfwTvhxNbbXl0jQpyy2EuXxTKRmdiVcepHuPR6LcL8slfzJYmaxf_aHjk89zEOQMdrzxj6PzEh4sxczIoH1oUIQl29vJkP9xRWSc8DJcvlwzSPglccRkENN2ih6DfWw4QwwAih92dVoBKkFO7rxxo2cqUYDdCk1VYKHcgmbsiNeY6lAG2btqS3FS54rzUC5PfyIaPNxE92pP-ttYVZfNnUgsHDtDSDYtfV3PGbT29G8d_vTI03pCqLZK44yJZufbUugiNLcYrGPllcDvsBqs1nptSltkBmAHVSPx_y3VzAm00)







































































































