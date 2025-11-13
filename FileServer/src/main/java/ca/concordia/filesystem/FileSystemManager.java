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

    public FileSystemManager(String filename, int totalSize) {
        if (instance != null) {
            throw new IllegalStateException("FileSystemManager is already initialized.");
        }
        try {
            inodeTable = new FEntry[MAXFILES];
            freeBlockList = new boolean[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) freeBlockList[i] = true;

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

    // delete name
    public void deleteFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            short b = fe.getFirstBlock();
            if (isValidBlock(b)) {
                zeroBlock(b);
                freeBlockList[b] = true;
            }
            inodeTable[idx] = null;
        } finally {
            globalLock.unlock();
        }
    }

    // write name/byte
    public void writeFile(String fileName, byte[] contents) throws Exception {
        if (contents == null) contents = new byte[0];
        if (contents.length > BLOCK_SIZE) throw new Exception("File is too big (max " + BLOCK_SIZE + " bytes)");

        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            short target = fe.getFirstBlock();
            if (!isValidBlock(target)) {
                int free = findFreeBlock();
                if (free == -1) throw new Exception("No free space");
                target = (short) free;
            }

            writeBlock(target, contents, 0, contents.length);

            if (contents.length < BLOCK_SIZE) {
                zeroBlockRange(target, contents.length, BLOCK_SIZE - contents.length);
            }

            fe.setFirstBlock(target);
            fe.setFilesize((short) contents.length);
            freeBlockList[target] = false;
        } finally {
            globalLock.unlock();
        }
    }

    // Read name
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
            if (!isValidBlock(b)) throw new Exception("File data missing");

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
        if (name == null || name.isEmpty()) throw new Exception("Invalid filename");
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
