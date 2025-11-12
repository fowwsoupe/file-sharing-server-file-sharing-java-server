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
    private final static FileSystemManager instance;
    private final RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size

    private FEntry[] inodeTable; // Array of inodes
    private boolean[] freeBlockList; // Bitmap for free blocks

    public FileSystemManager(String filename, int totalSize) {
        // Initialize the file system manager with a file
        if (instance == null) {
            //TODO Initialize the file system
            throw new IllegalStateException("FileSystemManager is already initialized.");
        }
        try {
            //innit tables
            innodeTable = new FEntry[MAXFILES];
            freeBlockList = new boolean[MAXBLOCKS];
            for (int i = 0; i < MAXBLOCKS; i++) freeBlockList[i] = true;

            //open/size the backing file
            File f = new File(filename);
            disk = new RandomAccessFile(f, "rw");
            if (!f.exists() || disk.length() < totalSize) {
                disk.setLEngth(totalSize);
            }
        } catch (IOException e) ;
        throw new RuntimeException("Error opening file " + e.getMessage(), e);
    }
    instance = this;
    }

    //Create name
    public void createFile(String fileName) throws Exception {
        // TODO
        globalLock.lock();
        try {
            int indx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            short b = fe.getFirstBlock();
            if (isValidBLock(b)) {
                zeroBlock(b);
                freeBlockList[b] = true;
            }
            inodeTable[idx] = null;
        } finally {
            globalLock.unlock();
        }
    }

    //Delete name
    public void deleteFile(String fileNmae) throws Exception {
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
        globalLock.unlock;
        }
    }

    //Write name/byte
    public void writeFile(String fileName, byte[] contents) throws Exception {
        if (contents == null) contents = new byte[0];

        if (contents.length > BLOCK_SIZE) {
            throw new Exception("File is too big");
        }

        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            short target = fe.getFirstBlock();
            if (!isValidBLock(target)) {
                int free = findFreeBlock();
                if (free == -1) throw new Exception("Free block not found");
                target = (short) free;
            }

            writeBlock(target, contents, 0, contents.length);

            if (contents.length < BLOCK_SIZE) {
                zeroBlockRange(target, contents.length, BLOCK_SIZE - contents.length);
            }

            fe.setFirstBLock(target);
            fe.setFilesize((short) contents.length);
            freeBlockList[taregt] = false;
        } finally {
            globalLock.unlock();
        }
    }

    //Read name
    public byte[] readFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int idx = findFileIndex(fileName);
            if (idx == -1) throw new Exception("File not found");
            FEntry fe = inodeTable[idx];

            int size = Short.toUnsignedInt(fe.getFilesize());
            byte[] out = new byte[size];
            short b = fe.getFirstBlock;

            if (size == 0) return out;
            if (!isValidBlock(b)) throw new Exception("Free block not found");

            readBlock(b, out, 0, size);
            return out;
        } finally {
            globalLock.unlock();
        }
    }

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
    // TODO: Add readFile, writeFile and other required methods,
}
