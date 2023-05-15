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

![image-20230515111921893](images/image-20230515111921893-16841207651191.png)

主要要实现：lru_replace.h中的这四个方法：

victim:删除没用节点

pin：表示有人在用

unpin：表示没人用，可以安全删除，多次unpin是无效的（或者认为是等效的）

size：记录目前有多少页，注意要小于buffer pool manager的大小

![image-20230515112105907](images/image-20230515112105907-16841208677932.png)

frame_id_t其实就是int32_t

 

**代码的修改：**

首先在lru_replacer.h中增加私有的成员，主要策略是用map和list来实现lru：

`unordered_map<frame_id_t,std::list<frame_id_t>::iterator>`第二个参数是链表节点。

![image-20230515142655696](images/image-20230515142655696-168413201723510.png)

补充一种奇怪的写法：

```cpp
auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { return false; }
```

**auto 关键字后面的函数调用表达式返回一个 bool 类型**,auto可以自动推导返回值的类型，，返回值显示返回值类型为bool

表示是否要替换当前缓存中的数据 (frame_id 所指的数据)。这里返回 false，表示不需要替换，因为当前缓存中的数据 (frame_id 所指的数据) 是最新的，且其优先级 (根据 LRU 算法) 高于需要被替换的数据。如果返回 true，则表示需要替换当前缓存中的数据，其中参数 frame_id 所指的数据将被替换。

**frame_id 是一个指针变量，它指向 LRUReplacer 类中缓存列表 (lru_list_) 中的最后一项。**

- 实现对应的Victim函数：

就是拿出最后的指针，然后把他从链表和map中删除，map中删除的是对应的索引。

![image-20230515120211338](images/image-20230515120211338-16841233328693.png)



- 实现对应的pin函数：(pin删除页的操作)

![image-20230515142957525](images/image-20230515142957525.png)



- 实现对应的unpin功能：

![image-20230515143543418](images/image-20230515143543418-168413254603811.png)

- 最后返回size：

![image-20230515143933559](images/image-20230515143933559.png)

