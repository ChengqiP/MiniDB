package com.pancq1037.miniDB.backend.dm;

import com.pancq1037.miniDB.backend.dm.dataItem.DataItem;
import com.pancq1037.miniDB.backend.dm.logger.Logger;
import com.pancq1037.miniDB.backend.dm.page.PageOne;
import com.pancq1037.miniDB.backend.dm.pageCache.PageCache;
import com.pancq1037.miniDB.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 加载并检查PageOne，如果检查失败，则进行恢复操作
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        // 填充PageIndex，遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中
        dm.fillPageIndex();
        // 设置PageOne为打开状态
        PageOne.setVcOpen(dm.pageOne);
        // 将PageOne立即写入到磁盘中，确保PageOne的数据被持久化
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
