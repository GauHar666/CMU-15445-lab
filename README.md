# 高涵的cmu15445 lab作业
lab0 ：p0_starter.h
## lab1 缓冲池

为BusTub DBMS构建一个新的面向磁盘的存储管理器。

项目的目的是为了在储存管理器中实现缓冲池。缓冲池**负责主内存和磁盘之间来回移动物理页。**

系统使用其唯一标识符 （`page_id_t`） 向缓冲池请求页面，并且不知道该页面是否已在内存中，或者系统是否必须从磁盘检索它。

要求：线程安全的。



### 任务1：LRU更换策略：

`src/include/buffer/lru_replacer.h` 中实现一个名为的新子类，并在 `src/buffer/lru_replacer.cpp` 中实现其相应的实现文件。

需要实现的几个方法：

- `Victim(frame_id_t*)`：删除与 跟踪的所有其他元素相比最近访问最少的对象，将其内容存储在输出参数中并返回 。如果为空，则返回 。`Replacer``True``Replacer``False`
- `Pin(frame_id_t)`：在将页面固定到框架后，应调用此方法。它应从 中删除包含固定页面的框架。`BufferPoolManager``LRUReplacer`
- `Unpin(frame_id_t)`：当页面变为 0 时，应调用此方法。此方法应将包含未固定页面的框架添加到 .`pin_count``LRUReplacer`
- `Size()`：此方法返回当前位于 中的帧数。`LRUReplacer`



基础知识的补充：

- 页：page
  - frame_id
  - Page_id
  - 上述二者的区别：一页是一段连续的地址空间，页的前面可以增加一些空间用来记录：比如这个页脏了吗，还有这个页目前的使用进程数。
- Struct Page的结构体内部的一些成员：
  - char data_[size]:也就是这个页的尺寸大小
  - bool dirty_:页面是否脏
  - int pin_count:目前使用这个页面的线程数，pin_count=0的时候才可以把这个页丢掉。
  - page_id：页的id
  - page_latch:页面的锁，保证原子性操作和解决并发问题



**LRU的补充：**

LRU主要会用在Buffer Pool Manager里面来做缓冲池管理。如何把buffer里面的页给替换掉

推荐实现方法：Map+LinkedList。双链表+哈希表。

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/9f5e4b61-9e7d-4b24-a199-cc6535342081)

主要要实现：lru_replace.h中的这四个方法：

victim:删除没用节点

pin：表示有人在用

unpin：表示没人用，可以安全删除，多次unpin是无效的（或者认为是等效的）

size：记录目前有多少页，注意要小于buffer pool manager的大小

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/9fec94f0-a6b3-435b-8527-3ca7a60f2c90)


frame_id_t其实就是int32_t

 

**代码的修改：**

首先在lru_replacer.h中增加私有的成员，主要策略是用map和list来实现lru：

`unordered_map<frame_id_t,std::list<frame_id_t>::iterator>`第二个参数是链表节点。

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/b3d74e2e-36cc-4a58-a836-1eff4d16400b)


补充一种奇怪的写法：

```cpp
auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { return false; }
```

**auto 关键字后面的函数调用表达式返回一个 bool 类型**,auto可以自动推导返回值的类型，，返回值显示返回值类型为bool

表示是否要替换当前缓存中的数据 (frame_id 所指的数据)。这里返回 false，表示不需要替换，因为当前缓存中的数据 (frame_id 所指的数据) 是最新的，且其优先级 (根据 LRU 算法) 高于需要被替换的数据。如果返回 true，则表示需要替换当前缓存中的数据，其中参数 frame_id 所指的数据将被替换。

**frame_id 是一个指针变量，它指向 LRUReplacer 类中缓存列表 (lru_list_) 中的最后一项。**

- 实现对应的Victim函数：

就是拿出最后的指针，然后把他从链表和map中删除，map中删除的是对应的索引。

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/73466a28-9263-4745-8bcf-bf0f7befb176)



- 实现对应的pin函数：(pin删除页的操作)

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/00a91b1d-6a28-499b-98bc-ddd00b677b2e)



- 实现对应的unpin功能：

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/c4f8d11f-ab88-4bb3-b457-637fd44c8842)

- 最后返回size：

![image](https://github.com/GauHar666/CMU-15445-lab/assets/100465185/68ceb27a-8ffe-4cda-b6a3-74e26630b1f9)

### 任务2：缓冲池管理器实例

负责从磁盘中获取数据库页，并将其存储在内存中

**已经提供了将数据读写入磁盘的代码：DiskManager中。**

系统中的所有内存中页都由对象表示。每个对象都包含一个内存块，该内存块将用作复制它从磁盘读取的**物理页**内容的位置。将重用同一对象来存储来回移动到磁盘的数据。这意味着同一对象在系统的整个生命周期中可能包含不同的物理页。

每个对象还维护一个计数器，用于“固定”该页面的线程数

您需要在源文件 （src/buffer/buffer_pool_manager_instance.cpp） 的头文件 （`src/include/buffer/buffer_pool_manager_instance.h`） 中实现以下函数：

- `FetchPgImp(page_id)`
- `UnpinPgImp(page_id, is_dirty)`
- `FlushPgImp(page_id)`
- `NewPgImp(page_id)`
- `DeletePgImp(page_id)`
- `FlushAllPagesImpl()`



关于这个lab的一些讲解：**为什么要有这个buffer pool manager，原因在于需要加快从磁盘的读写速度。**

<img src="images/image-20230515160448131.png" alt="image-20230515160448131" style="zoom:67%;" />

（BPM）首先要有一个`page* pages`的专门存放page，存放你的页，page里面又存在着data,每个page的里面那个具体的page页被称为frame_id，和page_id不同。**fram_id代表数组的索引，page_id代表在数据库中底层到底是第几页。**

所以就会产生一个页表，用来存储frame_id和page_id之间的关系：

```bash
page_table<page_id,frame_id>;
```



==对于源码中有一些我看不懂的写法做个补充：==

```cpp
void DeallocatePage(__attribute__((unused)) page_id_t page_id)   
```

**函数头中出现了`__attribute__((unused))`，这是一个编译器指令，表示告知编译器不使用该参数并防止出现未使用参数的警告。**



几个方法的实现：

- FetchPgImp：如果都被pin或者没有这一页就拿不到，如果有一个是unpin，那么就可以把它置换出来换成你想要的新页。
- upinpgimp：把这个页不用了。让内部的pincount--。并且坚持是否dirty，如果dirty要把其写回disk。
- newpageimpl：新建页。先从free_list中查找，找不到空闲的再用LRU踢掉。
- delete：删除页，如果脏页就更新一下
- flush：刷新






实现：

对于buffer__pool_manager_instance.h头文件：

![image-20230515163232773](images/image-20230515163232773-168413965359214.png)

私有成员变量：

![image-20230515163744107](images/image-20230515163744107-168413986563716.png)

**很重要的一个是page_table_,是一个unordered_map，记录了page_id_t和frame_id_t之间的映射**。

首先构造函数：

要构造好pages数组和LRUreplacer的初始值：

free_list的大小和page的大小设置为相同,然后同时要让free_list先填充上pool_size的大小：

```cpp
pages_ = new Page[pool_size_];
replacer_ = new LRUReplacer(pool_size);
for(size_t i=0;i<pool_size;i++){
    free_list_.empalce_back(static_cast<int>(1));
}
```

析构：主要释放pages和replacer

```cpp
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
	delete[] pages_;
    delete replacer_;
}
```

- FlushPgImp函数：刷新这一页，不管脏不脏都写回，原本写错了，以为只有脏页才写回

```cpp
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if(page_table_.count(page_id)==0){
    return false;
  }
  frame_id_t frame_id=page_table_[page_id];
  disk_manager_->WritePage(page_id,pages_[frame_id].data_);
  pages_[frame_id].is_dirty_=false;
  return true;
}
```

- FlushAllPgsImp函数：

```cpp
void BufferPoolManagerInstance::FlushAllPgsImp() {
  page_id_t page_id;
  frame_id_t frame_id;
  std::lock_guard<std::mutex> lock(latch_);
  for(auto& item :page_table_){
    page_id=item.first;
    frame_id=item.second;
    disk_manager_->WritePage(page_id,pages_[frame_id].data_);
    pages_[frame_id].is_dirty_=false;
  }
}
```

- GetFrame函数：

![image-20230515174154629](images/image-20230515174154629-168414371612017.png)

- NewPgImp函数：

注意点在于：一定要将==创建新页也需要写回磁盘，如果不这样 newpage unpin 然后再被淘汰出去 fetchpage时就会报错（磁盘中并无此页）==

创建了新页之后要同步更新page_table_内部值，也要更新`pages_`内部的值

```cpp
frame_id_t frame_id;
  page_id_t new_page_id;
  std::lock_guard<std::mutex> lock(latch_);
  frame_id=GetFrame();
  if(frame_id==NUMLL_FRAME) return nullptr;
  new_page_id=AllocatePage();
  page_table_[new_page_id]=frame_id;//更新pagetable
  pages_[frame_id].page_id_=new_page_id;
  pages_[frame_id].is_dirty_=false;
  pages_[frame_id].pin_count_=1;
  pages_[frame_id].ResetMemory();

  disk_manager_->WritePage(pages_[frame_id].page_id_,pages_[frame_id].data_);
  *page_id = new_page_id;
  return &pages_[frame_id];
```

- AllocatePage（）函数：

利用next_page_id为目前的pages_ 页面数组中分配下一页。

![image-20230516112305197](images/image-20230516112305197-168420738710521.png)



- FetchPgImp函数：

拿一页的方法。

要分两类讨论：

- 原本就在buffer里面
- 原本不在buffer里面，那么要用getframe函数拿一页出来。

```cpp
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id;
  std::lock_guard<std::mutex> lock(latch_);  // 加锁
  if (page_table_.count(page_id) > 0) {      // 原先就在buffer里
    frame_id = page_table_[page_id];
    // 只有pin_count为0才有可能在replacer里
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->Pin(frame_id);
    }
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  frame_id = GetFrame();
  if (frame_id == NUMLL_FRAME) {
    return nullptr;
  }
  page_table_[page_id] = frame_id;

  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  return &pages_[frame_id];
}
```



- DeletePgImp

<img src="images/image-20230516114929737.png" alt="image-20230516114929737" style="zoom:67%;" />
