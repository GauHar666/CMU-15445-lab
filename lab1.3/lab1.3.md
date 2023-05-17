### 任务3 并行缓冲池管理器

在上一个任务中可能注意到，单个缓冲池管理器实例需要采用锁才可以保证线程安全。为了每个线程在与缓冲池交互时都要争夺锁的情况，一种解决方案是有多个缓冲池，每个池有自己的锁就好了。

我们会给page id来确定哪个具体的页被使用。那么我们需要一些方法来将给定的页面ID映射到[0，num_instances范围内的数字。对于这个项目，我们将使用取模运算符，将给定的page_id映射到正确的范围。

在源文件`buffer_pool_manager_instance.cpp`和`buffer_pool_manager_instance.h`中实现：

- `ParallelBufferPoolManager(num_instances, pool_size, disk_manager, log_manager)`
- `~ParallelBufferPoolManager()`
- `GetPoolSize()`
- `GetBufferPoolManager(page_id)`
- `FetchPgImp(page_id)`
- `UnpinPgImp(page_id, is_dirty)`
- `FlushPgImp(page_id)`
- `NewPgImp(page_id)`
- `DeletePgImp(page_id)`
- `FlushAllPagesImpl()`

这个任务总体还是比较简单，只需要调用前面的几个接口就可以了。



**对于instannces_实例来说，因为这里是很多并发的buffer_manager，所以我们需要创建很多的bufferpoolmanager实例来进行，==对于实例的存储，我们使用vector里面用共享指针的方式，==到时候就可以自动释放内存。析构就不用析构了**

```cpp
std::vector<std::shared_ptr<BufferPoolManager>> instance_;
```

传入的num_instances就是instances_数组的大小，所以在传入时的初始化：

```cpp
instance_.resize(num_instances);
for(int i=0;i<num_instances;i++){
    instances_[i] = std::make_shared<BufferPoolManagerInstance>(pool_size,num_instances,disk_manager,log_manager);
  }
```





其他的一些私有成语：

<img src="images/image-20230517140254037.png" alt="image-20230517140254037" style="zoom:80%;" />



- GetPoolSize函数：

要返回所有实例的总和：

```cpp
return pool_size_*num_instance_
```

- GetBufferPoolManager

```cpp
return instances_[page_id % number_instances_].get();
```

这里的.get()方法是share`.get()` 方法是一个 C++11 引入的 shared_ptr 函数，用于返回管理的原始指针。换句话说，它返回共享指针所拥有的对象的实际指针。

**返回的是一个指向该智能指针所持有的内存地址的原始指针。**

- FetchPgImp函数：

```c++
BufferPoolManger *manager = GetBufferPoolManager(page_id);
return manager->FetchPage(page_id);
```

别的几个函数都和上面大同小异。都是先创建一个新的manager对象，利用GetBufferPoolManager，然后再调用manager里面的函数。



- 对于NewPgImp来说有点不一样，要判断在哪个instances里面创建：

```cpp
//对于newpgimp有点不一样，要根据start_instance的位置来判断新的page要放在哪个instance下面
  Page *page;
  auto index = start_index_;
  //注意要判断在这个instance中会不会创建失败，如果创建失败就到下一个instance中去创建
  for(int i=index;i<=2*index;i++){
    page=instances_[i]->NewPage(page_id);
    if(page==nullptr){
      break;
    }
    index = (index+1)%num_instances_;
  }
  start_index_ = (start_index_+1)%num_instances_;
  return page;
```

