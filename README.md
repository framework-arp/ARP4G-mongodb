# ARP4G-mongodb
这是[ARP4G](https://github.com/zhengchengdong/ARP4G)的MongoDB实现。

## 功能
1. 实现基于MongoDB的持久化
2. 实现基于MongoDB的互斥锁
## 如何使用
```go
//定义仓库
type OrderRepository interface {
	Take(ctx context.Context, id any) (order *Order, found bool)
}

//定义Service
type OrderService struct {
	orderRepository OrderRepository
}

//获得mongodb客户端
mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(
	"mongodb://localhost:27017/orders"))
if err != nil {
	panic(err)
}

//生成仓库的mongodb实现
mongoOrderRepo := mongorepo.NewMongodbRepository(mongoClient, "orders", "Order", func() *Order { return &Order{} })

//使用仓库生成Service
orderService := &OrderService{mongoOrderRepo}
```
这里使用**NewMongodbRepository**打包生成了MongoDB的仓库实现，仓库会使用MongoDB持久化，也会用MongoDB实现互斥锁。

```go
func NewMongodbRepositoryWithMutexesimpl[T any](client *mongo.Client, database string, collection string, newEmptyEntity arp.NewZeroEntity[T], mutexesimpl arp.Mutexes) arp.QueryRepository[T]
```
也可以使用**NewMongodbRepositoryWithMutexesimpl**函数选择不同的互斥锁实现，比如，持久化到MongoDB，但是用Redis实现互斥锁
