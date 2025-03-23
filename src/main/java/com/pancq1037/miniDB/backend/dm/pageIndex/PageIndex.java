package com.pancq1037.miniDB.backend.dm.pageIndex;

import com.pancq1037.miniDB.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists; // 空闲空间页信息列表

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            /*
               计算空闲空间对应的区间编号。THRESHOLD 是每个区间的大小，通过 freeSpace 除以 THRESHOLD 得出区间编号
               这里int number = freeSpace / THRESHOLD; 已经向下取整，例如，空闲空间为2005，不能容纳2040大小的数据
               因此只能向下取整，能容纳2000的对应lists索引为2000/40=50，lists[50]会存PageInfo
             */
            int number = freeSpace / THRESHOLD;
            // 根据计算出的区间编号，将包含页号（pgno）和空闲空间大小（freeSpace）的 PageInfo 对象添加到对应区间的列表中
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }
    /**
     * 根据给定的空间大小选择一个 PageInfo 对象。
     *
     * @param spaceSize 需要的空间大小
     * @return 一个 PageInfo 对象，其空闲空间大于或等于给定的空间大小。如果没有找到合适的 PageInfo，返回 null。
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 计算所需空间大小对应的区间编号
            // THRESHOLD 是每个区间的大小，通过 spaceSize 除以 THRESHOLD 得到区间编号
            int number = spaceSize / THRESHOLD;
            // 如果计算得到的区间编号小于最大区间编号 INTERVALS_NO，将区间编号加 1，此处+1主要为了向上取整
            // 这样做是为了优先从更大的空闲空间区间开始查找，以确保有足够的空间
            /*
                1、假需要存储的字节大小为5168，此时计算出来的区间号是25，但是25*204=5100显然是不满足条件的
                2、此时向上取整找到 26，而26*204=5304，是满足插入条件的
            */
            if(number < INTERVALS_NO) number ++;
            // 从计算出的区间编号开始，向上寻找合适的 PageInfo
            while(number <= INTERVALS_NO) {
                // 检查当前区间对应的列表中是否有元素
                if(lists[number].size() == 0) {
                    // 如果当前区间对应的列表为空，说明该区间[number*40:number*40+40]没有合适的空闲页
                    // 将区间编号加 1，继续检查下一个区间
                    number ++;
                    continue;
                }
                // 如果当前区间对应的列表不为空，说明找到了合适的空闲页
                // 移除列表中的第一个元素，并返回该元素（说明该页已经用来存储一个新的数据）
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
