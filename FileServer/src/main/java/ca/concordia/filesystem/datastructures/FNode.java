package ca.concordia.filesystem.datastructures;

public class FNode {
    public static final short FREE = -1;
    public static final short NO_NEXT = -1;

    private short blockIndex;
    private short nextBlock;

    public FNode() {
        this.blockIndex = FREE;
        this.nextBlock = NO_NEXT;
    }

    public FNode(int blockIndex) {
        this.blockIndex = (short)blockIndex;
        this.nextBlock = NO_NEXT;
    }

    public short getBlockIndex() {
        return blockIndex;
    }

    public void setBlockIndex(short blockIndex) {
        this.blockIndex = blockIndex;
    }

    public short getNextBlock() {
        return nextBlock;
    }

    public void setNextBlock(short nextBlock) {
        this.nextBlock = nextBlock;
    }

    public boolean isFree() {
        return this.blockIndex == FREE;
    }

    public void reset(){
        this.blockIndex = FREE;
        this.nextBlock = NO_NEXT;
    }

    @Override
    public String toString() {
        return "FNode (block=" + blockIndex + ", next=" + nextBlock + ")";
    }

}
