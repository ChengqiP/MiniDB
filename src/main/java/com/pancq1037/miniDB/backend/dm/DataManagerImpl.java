package com.pancq1037.miniDB.backend.dm;

import com.pancq1037.miniDB.backend.common.AbstractCache;
import com.pancq1037.miniDB.backend.dm.dataItem.DataItem;
import com.pancq1037.miniDB.backend.dm.dataItem.DataItemImpl;
import com.pancq1037.miniDB.backend.dm.logger.Logger;
import com.pancq1037.miniDB.backend.dm.page.Page;
import com.pancq1037.miniDB.backend.dm.page.PageOne;
import com.pancq1037.miniDB.backend.dm.page.PageX;
import com.pancq1037.miniDB.backend.dm.pageCache.PageCache;
import com.pancq1037.miniDB.backend.dm.pageIndex.PageIndex;
import com.pancq1037.miniDB.backend.dm.pageIndex.PageInfo;
import com.pancq1037.miniDB.backend.tm.TransactionManager;
import com.pancq1037.miniDB.backend.utils.Panic;
import com.pancq1037.miniDB.backend.utils.Types;
import com.pancq1037.miniDB.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        //从缓存页面中读取到DataItemImpl
        DataItemImpl di = (DataItemImpl)super.get(uid);
        //校验di是否有效
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将输入的数据包装成DataItem的原始格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 如果数据项的大小超过了页面的最大空闲空间，抛出异常
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 初始化一个页面信息对象
        PageInfo pi = null;
        // 尝试5次找到一个可以容纳新数据项的页面
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            // 如果找到了合适的页面，跳出循环
            if (pi != null) {
                break;
            } else {
                // 如果没有找到合适的页面，创建一个新的页面，并将其添加到页面索引中
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        // 如果还是没有找到合适的页面，抛出异常
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }
        // 初始化一个页面对象
        Page pg = null;
        // 初始化空闲空间大小为0
        int freeSpace = 0;
        try {
            // 获取页面信息对象中的页面
            pg = pc.getPage(pi.pgno);
            // 生成插入日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            // 将日志写入日志文件
            logger.log(log);
            // 在页面中插入新的数据项，并获取其在页面中的偏移量
            short offset = PageX.insert(pg, raw);
            // 释放页面
            pg.release();
            // 返回新插入的数据项的唯一标识符
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        // 需要执行缓存和日志的关闭流程，
        super.close();
        logger.close();
        // 还需要设置第一页的字节校验
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 填充 PageIndex。
     * 遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中。
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            // 将页面编号和页面的空闲空间大小添加到 PageIndex 中
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            // 注意在使用完 Page 后需要及时 release，否则可能会撑爆缓存
            pg.release();
        }
    }
    
}
