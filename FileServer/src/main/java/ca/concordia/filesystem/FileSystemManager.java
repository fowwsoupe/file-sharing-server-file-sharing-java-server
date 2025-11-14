package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private static FileSystemManager instance;

    private RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128;

    private FEntry[] inodeTable;
    private boolean[] freeBlockList;

    private FNode[] fnodes;

    public FileSystemManager(String filename, int totalSize) {
        if (instance != null) {
            throw new IllegalStateException("FileSystemManager is already initialized.");
        }
        try {
            inodeTable = new FEntry[MAXFILES];
            freeBlockList = new boolean[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) {
                freeBlockList[i] = true;
            }

            fnodes = new FNode[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) {
                fnodes[i] = new FNode();
            }

            File f = new File(filename);
            disk = new RandomAccessFile(f, "rw");
            if (!f.exists() || disk.length() < totalSize) {
                disk.setLength(totalSize);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error opening file: " + e.getMessage(), e);
        }
        instance = this;
    }

    // create name
    public void createFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            ensureValidName(fileName);
            if (findFileIndex(fileName) != -1) throw new Exception("File exists");
            int slot = findFreeInode();
            if (slot == -1) throw new Exception("No free file entries");
            FEntry fe = new FEntry(fileName);
            fe.setFilesize((short) 0);
            fe.setFirstBlock((short) -1);
            inodeTable[slot] = fe;
        } finally {
            globalLock.unlock();
        }
    }

    // delete nmae
    public void deleteFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            short firstFNode = fe.getFirstBlock();
            if (firstFNode >= 0) {
                freeChain(firstFNode, true);
            }
            inodeTable[idx] = null;
        } finally {
            globalLock.unlock();
        }
    }

    // write name/byte method
    public void writeFile(String fileName, byte[] contents) throws Exception {
        if (contents == null) contents = new byte[0];
        if (contents.length > BLOCK_SIZE) throw new Exception("File is too big (max " + BLOCK_SIZE + " bytes)");

        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("Ifle not found");
            FEntry fe = inodeTable[idx];

            int size = contents.length;

            int maxBytes = MAXBLOCKS * BLOCK_SIZE;
            if (size > maxBytes) {
                throw new Exception("File is too big (max " + maxBytes + " bytes)");
            }

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
                freeChain(oldFirst, false); // free old blocks/nodes, no need to zero
            }

            int[] fnodeIdx = new int[neededBlocks];
            int[] blockIdx = new int[neededBlocks];

            for (int i = 0; i < neededBlocks; i++) {
                int fn = findFreeFNode();
                int blk = findFreeBlock();
                if (fn == -1 || blk == -1) {
                    throw new Exception("no free space");
                }
                fnodeIdx[i] = fn;
                blockIdx[i] = blk;

                freeBlockList[blk] = false;
                fnodes[fn].setBlockIndex((short) blk);
            }

            for (int i = 0; i < neededBlocks; i++) {
                if (i == neededBlocks - 1) {
                    fnodes[fnodeIdx[i]].setNextBlock(FNode.NO_NEXT);
                } else {
                    fnodes[fnodeIdx[i]].setNextBlock((short) fnodeIdx[i + 1]);
                }
            }

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
            globalLock.unlock();
        }
    }

    // Read name method
    public byte[] readFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            int size = Short.toUnsignedInt(fe.getFilesize());
            byte[] out = new byte[size];
            short b = fe.getFirstBlock();

            if (size == 0) return out;
            if (!isValidBlock(b)) throw new Exception("data missing");

            readBlock(b, out, 0, size);
            return out;
        } finally {
            globalLock.unlock();
        }
    }

    //List method
    public String[] listFiles() {
        globalLock.lock();
        try {
            ArrayList<String> names = new ArrayList<>();
            for (FEntry fe : inodeTable) {
                if (fe != null) names.add(fe.getFilename());
            }
            return names.toArray(new String[0]);
        } finally {
            globalLock.unlock();
        }
    }

    // Helper category::

    private void ensureValidName(String name) throws Exception {
        if (name == null || name.isEmpty()) throw new Exception("Invalid File Name");
    }

    private int findFileIndex(String name) {
        for (int i = 0; i < MAXFILES; i++) {
            if (inodeTable[i] != null && inodeTable[i].getFilename().equals(name)) return i;
        }
        return -1;
    }

    private int findFreeInode() {
        for (int i = 0; i < MAXFILES; i++) if (inodeTable[i] == null) return i;
        return -1;
    }

    private int findFreeBlock() {
        for (int i = 0; i < MAXBLOCKS; i++) if (freeBlockList[i]) return i;
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
                for (int i = start; i < off + len; i++) dst[i] = 0;
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
            throw new Exception("Zero-fill failed");
        }
    }

}
