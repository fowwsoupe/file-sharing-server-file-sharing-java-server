package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private static final int BLOCK_SIZE = 128;

    private final RandomAccessFile disk;

    private final FEntry[] inodeTable;
    private final boolean[] freeBlockList;
    private final FNode[] fnodes;

    //reader/writer lock
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    //Shortcuts references for read/write access
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public FileSystemManager(String filename, int totalSize) {
        try {
            //create empty inode table
            inodeTable = new FEntry[MAXFILES];
            freeBlockList = new boolean[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) {
                freeBlockList[i] = true;
            }

            fnodes = new FNode[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) {
                fnodes[i] = new FNode();
            }

            //open the disk file
            File f = new File(filename);
            disk = new RandomAccessFile(f, "rw");
            //check if disk file is correct size
            if (!f.exists() || disk.length() < totalSize) {
                disk.setLength(totalSize);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error opening file: " + e.getMessage(), e);
        }
    }

    // create <filename>
    public void createFile(String fileName) throws Exception {
        //lock around the critical section
        writeLock.lock();
        try {
            ensureValidName(fileName);

            //Check if file already exists
            int existing = findFileIndex(fileName);
            if (existing != -1) {
                return;
            }

            //Finds a free slot in the inode table
            int slot = findFreeInode();
            if (slot == -1) {
                throw new Exception("No free file entries");
            }

            //create a new inode
            FEntry fe = new FEntry(fileName);
            fe.setFilesize((short) 0);
            fe.setFirstBlock((short) -1);
            inodeTable[slot] = fe;
        } finally {
            writeLock.unlock();
        }
    }

    // delete <filename>
    public void deleteFile(String fileName) throws Exception {
        writeLock.lock();
        try {
            //search for the file in the inode entry
            int idx = findFileIndex(fileName);
            if (idx == -1) {
                throw new Exception("File not found");
            }
            FEntry fe = inodeTable[idx];

            short firstFNode = fe.getFirstBlock();
            if (firstFNode >= 0) {
                freeChain(firstFNode, true);
            }

            //remove the inode entry from the table
            inodeTable[idx] = null;
        } finally {
            writeLock.unlock();
        }
    }

    // write <filename> <contents>
    public void writeFile(String fileName, byte[] contents) throws Exception {
        if (contents == null) {
            contents = new byte[0];
        }

        writeLock.lock();
        try {
            //find the inode for the file
            int idx = findFileIndex(fileName);
            if (idx == -1) {
                throw new Exception("File not found");
            }
            FEntry fe = inodeTable[idx];

            int size = contents.length;
            int maxBytes = MAXBLOCKS * BLOCK_SIZE;
            if (size > maxBytes) {
                throw new Exception("File is too big (max " + maxBytes + " bytes)");
            }

            //clear the file
            if (size == 0) {
                short oldFirst = fe.getFirstBlock();
                if (oldFirst >= 0) {
                    freeChain(oldFirst, false);
                }
                fe.setFirstBlock((short) -1);
                fe.setFilesize((short) 0);
                return;
            }

            int neededBlocks = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

            short oldFirst = fe.getFirstBlock();
            if (oldFirst >= 0) {
                freeChain(oldFirst, false);
            }

            int[] fnodeIdx = new int[neededBlocks];
            int[] blockIdx = new int[neededBlocks];

            for (int i = 0; i < neededBlocks; i++) {
                int fn = findFreeFNode();
                int blk = findFreeBlock();

                if (fn == -1 || blk == -1) {
                    for (int j = 0; j < i; j++) {
                        int blkUsed = blockIdx[j];
                        if (blkUsed >= 0) {
                            freeBlockList[blkUsed] = true;
                        }
                        fnodes[fnodeIdx[j]].reset();
                    }
                    throw new Exception("no free space");
                }

                fnodeIdx[i] = fn;
                blockIdx[i] = blk;

                freeBlockList[blk] = false;
                fnodes[fn].setBlockIndex((short) blk);
            }

            // Link fnodes into a chain (like linked list)
            for (int i = 0; i < neededBlocks; i++) {
                if (i == neededBlocks - 1) {
                    fnodes[fnodeIdx[i]].setNextBlock(FNode.NO_NEXT);
                } else {
                    fnodes[fnodeIdx[i]].setNextBlock((short) fnodeIdx[i + 1]);
                }
            }

            //this ap^rt allows to write
            int offset = 0;
            for (int i = 0; i < neededBlocks; i++) {
                short blk = (short) blockIdx[i];
                int remaining = size - offset;
                int len = Math.min(BLOCK_SIZE, remaining);

                writeBlock(blk, contents, offset, len);

                if (len < BLOCK_SIZE) {
                    zeroBlockRange(blk, len, BLOCK_SIZE - len);
                }

                offset += len;
            }

            fe.setFirstBlock((short) fnodeIdx[0]);
            fe.setFilesize((short) size);
        } finally {
            writeLock.unlock();
        }
    }

    // read <filename>
    public byte[] readFile(String fileName) throws Exception {
        readLock.lock();
        try {
            //find the inode for the file
            int idx = findFileIndex(fileName);
            if (idx == -1) {
                throw new Exception("File not found");
            }
            FEntry fe = inodeTable[idx];

            //determine the size of the file
            int size = Short.toUnsignedInt(fe.getFilesize());
            byte[] out = new byte[size];
            if (size == 0) {
                return out;
            }

            short fnodeIndex = fe.getFirstBlock();
            if (fnodeIndex < 0) {
                throw new Exception("data missing");
            }

            int offset = 0;
            //traverse the linked list to reqd the file in order
            while (fnodeIndex >= 0 && offset < size) {
                FNode node = fnodes[fnodeIndex];
                short blk = node.getBlockIndex();
                if (!isValidBlock(blk)) {
                    throw new Exception("data missing");
                }

                int remaining = size - offset;
                int len = Math.min(BLOCK_SIZE, remaining);

                readBlock(blk, out, offset, len);
                offset += len;

                short next = node.getNextBlock();
                if (next == FNode.NO_NEXT) {
                    break;
                }
                fnodeIndex = next;
            }

            return out;
        } finally {
            readLock.unlock();
        }
    }

    // list
    public String[] listFiles() {
        readLock.lock();
        try {
            ArrayList<String> names = new ArrayList<>();
            //inode table represents the filesystem directory structure
            for (FEntry fe : inodeTable) {
                if (fe != null) {
                    names.add(fe.getFilename());
                }
            }
            return names.toArray(new String[0]);
        } finally {
            readLock.unlock();
        }
    }

    //Helper section for the methods

    private void ensureValidName(String name) throws Exception {
        //check if file is empty or not
        if (name == null || name.isEmpty()) {
            throw new Exception("Invalid filename");
        }
        if (name.length() > 11) {
            throw new Exception("Filename is too long");
        }
    }

    private int findFileIndex(String name) {
        for (int i = 0; i < MAXFILES; i++) {
            if (inodeTable[i] != null && inodeTable[i].getFilename().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private int findFreeInode() {
        for (int i = 0; i < MAXFILES; i++) {
            if (inodeTable[i] == null) {
                return i;
            }
        }
        return -1;
    }

    private int findFreeBlock() {
        for (int i = 0; i < MAXBLOCKS; i++) {
            if (freeBlockList[i]) {
                return i;
            }
        }
        return -1;
    }

    private int findFreeFNode() {
        for (int i = 0; i < MAXBLOCKS; i++) {
            if (fnodes[i].isFree()) {
                return i;
            }
        }
        return -1;
    }

    private boolean isValidBlock(short b) {
        return b >= 0 && b < MAXBLOCKS;
    }

    private long offsetOf(short blockIndex) {
        return (long) blockIndex * BLOCK_SIZE;
    }

    private void writeBlock(short blockIndex, byte[] src, int off, int len) throws Exception {
        try {
            disk.seek(offsetOf(blockIndex));
            disk.write(src, off, len);
        } catch (IOException e) {
            throw new Exception("Disk write failed");
        }
    }

    private void readBlock(short blockIndex, byte[] dst, int off, int len) throws Exception {
        try {
            disk.seek(offsetOf(blockIndex));
            int n = disk.read(dst, off, len);
            if (n < len) {
                int start = off + Math.max(n, 0);
                for (int i = start; i < off + len; i++) {
                    dst[i] = 0;
                }
            }
        } catch (IOException e) {
            throw new Exception("Disk read failed");
        }
    }

    private void zeroBlock(short blockIndex) throws Exception {
        zeroBlockRange(blockIndex, 0, BLOCK_SIZE);
    }

    private void zeroBlockRange(short blockIndex, int start, int count) throws Exception {
        try {
            disk.seek(offsetOf(blockIndex) + start);
            byte[] zeros = new byte[count];
            disk.write(zeros);
        } catch (IOException e) {
            throw new Exception("zero-fill failed");
        }
    }

    private void freeChain(short firstFNode, boolean zeroData) throws Exception {
        short current = firstFNode;

        //follows the linked list of fnode until the last node
        while (current >= 0 && current < MAXBLOCKS) {
            FNode node = fnodes[current];
            short blk = node.getBlockIndex();
            short next = node.getNextBlock();

            if (isValidBlock(blk)) {
                if (zeroData) {
                    zeroBlock(blk);
                }
                freeBlockList[blk] = true;
            }

            node.reset();

            if (next == FNode.NO_NEXT) {
                break;
            }
            current = next;
        }
    }
}

