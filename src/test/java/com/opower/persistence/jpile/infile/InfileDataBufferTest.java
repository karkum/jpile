package com.opower.persistence.jpile.infile;

import java.io.IOException;
import java.io.InputStreamReader;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for the InfileDataBuffer
 *
 * @author aaron.silverman
 */
public class InfileDataBufferTest {

    private InfileDataBuffer infileDataBuffer;

    @Before
    public void setUp() {
        infileDataBuffer = new InfileDataBuffer();
    }

    @Test
    public void testAppendString() {
        String contents = "Gladiator is the best movie ever!";
        infileDataBuffer.append(contents);
        addRowAndAssertContents(contents);
    }

    @Test
    public void testAppendStringNeedingEscaping() {
        String contents = "C:\\windows\\bluescreen.png";
        infileDataBuffer.append(contents);
        addRowAndAssertContents(contents.replace("\\", "\\\\"));
    }

    @Test
    public void testAppendByte() {
        infileDataBuffer.append((byte) 65);
        addRowAndAssertContents("A");
    }

    @Test
    public void testAppendByteNeedingEscaping() {
        infileDataBuffer.append((byte) 92);
        addRowAndAssertContents("\\\\");
    }

    @Test
    public void testAppendBytes() {
        byte[] bytes = {72, 101, 108, 108, 111, 33};
        infileDataBuffer.append(bytes);
        addRowAndAssertContents("Hello!");
    }

    @Test
    public void testAppendBytesNeedingEscaping() {
        byte[] bytes = {67, 58, 92};
        infileDataBuffer.append(bytes);
        addRowAndAssertContents("C:\\\\");
    }

    @Test
    public void testAppendNull() throws Exception {
        infileDataBuffer.appendNull();
        addRowAndAssertContents("\\N");
    }

    private void addRowAndAssertContents(String expected) {
        try {
            infileDataBuffer.addRowToInfile();
            assertEquals(expected, CharStreams.toString(new InputStreamReader(infileDataBuffer.asInputStream())));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }
}
