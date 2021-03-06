package com.opower.persistence.jpile.infile;

import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for the InfileDataBuffer
 *
 * @author aaron.silverman
 */
public class InfileDataBufferTest {
    private InfileDataBuffer infileDataBuffer;
    private static final String TIMESTAMP_STRING = "2000-01-10 08:00:01";
    private static final String DATE_STRING = "2000-01-10";
    private static final String TIME_STRING = "08:00:01";
    private static final Date TEST_DATE = new Date(100, 0 , 10, 8 , 0, 1);

    @Before
    public void setUp() {
        this.infileDataBuffer = new InfileDataBuffer();
    }

    @Test
    public void testAppendString() {
        String contents = "Gladiator is the best movie ever!";
        this.infileDataBuffer.append(contents);
        addRowAndAssertContents(contents);
    }

    @Test
    public void testAppendStringNeedingEscaping() {
        String contents = "C:\\windows\\bluescreen.png";
        this.infileDataBuffer.append(contents);
        addRowAndAssertContents(contents.replace("\\", "\\\\"));
    }

    /**
     * Verify that the {@link InfileDataBuffer#append(String)} method correctly escapes special characters multiple special
     * characters are present in the input String.
     */
    @Test
    public void testAppendStringNeedEscapingWithMultipleEscapeCharacters() {
        String input = "D\ba\nv\ri\td\0D\\D\u001A";
        String expected = "D\\\ba\\\nv\\\ri\\\td\\\0D\\\\D\\\u001A";

        this.infileDataBuffer.append(input);
        addRowAndAssertContents(expected);
    }

    /**
     * Verify that the {@link InfileDataBuffer#append(String)} method correctly escapes special characters special characters
     * are back to back in the input String.
     */
    @Test
    public void testAppendStringNeedEscapingBackToBackEscapeCharacters() {
        String input = "Dav\r\nidDD\u001A\u001A";
        String expected = "Dav\\\r\\\nidDD\\\u001A\\\u001A";

        this.infileDataBuffer.append(input);
        addRowAndAssertContents(expected);
    }

    @Test
    public void testAppendByte() {
        this.infileDataBuffer.append((byte) 65);
        addRowAndAssertContents("A");
    }

    @Test
    public void testAppendByteNeedingEscaping() {
        this.infileDataBuffer.append((byte) 92);
        addRowAndAssertContents("\\\\");
    }

    @Test
    public void testAppendBytes() {
        byte[] bytes = {72, 101, 108, 108, 111, 33};
        this.infileDataBuffer.append(bytes);
        addRowAndAssertContents("Hello!");
    }

    @Test
    public void testAppendBytesNeedingEscaping() {
        byte[] bytes = {67, 58, 92};
        this.infileDataBuffer.append(bytes);
        addRowAndAssertContents("C:\\\\");
    }

    /**
     * Attempt to insert a row of empty data in between two rows of good data. We should not
     * have a row in the infile buffer for the empty row in the middle.
     */
    @Test
    public void testAppendWithEmptyRowBuffer() {
        String contents1 = "GO SKINS";
        String contents2 = "GO WIZ";

        this.infileDataBuffer.append(contents1);
        this.infileDataBuffer.addRowToInfile();

        this.infileDataBuffer.newRow();
        this.infileDataBuffer.append("");
        this.infileDataBuffer.addRowToInfile();

        this.infileDataBuffer.newRow();
        this.infileDataBuffer.append(contents2);
        addRowAndAssertContents(contents1 + "\n" + contents2);
    }

    @Test
    public void testAppendNull() throws Exception {
        this.infileDataBuffer.appendNull();
        addRowAndAssertContents("\\N");
    }

    @Test(expected = NullPointerException.class)
    public void testTemporalAnnotationTestClass() throws NoSuchMethodException {
        this.infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getDate", null));
    }

    @Test
    public void testTemporalDateAnnotation() throws NoSuchMethodException {
        this.infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getDateWithTemporal", null));
        addRowAndAssertContents(DATE_STRING);
    }

    @Test
    public void testTemporalTimeAnnotation() throws NoSuchMethodException {
        this.infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getTimeWithTemporal", null));
        addRowAndAssertContents(TIME_STRING);
    }

    @Test
    public void testTemporalTimestampAnnotation() throws NoSuchMethodException {
        this.infileDataBuffer.append(TEST_DATE, TemporalAnnotationTestClass.class.getMethod("getTimestampWithTemporal", null));
        addRowAndAssertContents(TIMESTAMP_STRING);
    }

    @Test
    public void testNullDateWithTemporal() throws NoSuchMethodException {
        this.infileDataBuffer.append(null, TemporalAnnotationTestClass.class.getMethod("getTimestampWithTemporal", null));
        addRowAndAssertContents("\\N");
    }

    @Test
    public void testFloatWithPrecisionAndScale1() {
        this.infileDataBuffer.append(84009.469f, 12, 3);
        addRowAndAssertContents("84009.469");
    }

    @Test
    public void testFloatWithPrecisionAndScale2() {
        this.infileDataBuffer.append(182921.969f, 12, 3);
        addRowAndAssertContents("182921.969");
    }

    @Test
    public void testFloatWithPrecisionAndScale3() {
        this.infileDataBuffer.append(16725.617f, 12, 3);
        addRowAndAssertContents("16725.617");
    }

    private void addRowAndAssertContents(String expected) {
        try {
            this.infileDataBuffer.addRowToInfile();
            assertEquals(expected, CharStreams.toString(new InputStreamReader(this.infileDataBuffer.asInputStream())));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Entity
    @Table
    private class TemporalAnnotationTestClass {
        private Date date;
        private Date dateWithTemporal;
        private Date timeWithTemporal;
        private Date timestampWithTemporal;

        @Column(name = "date")
        public Date getDate() {
            return this.date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        @Temporal(TemporalType.DATE)
        @Column(name = "date_with_temporal")
        public Date getDateWithTemporal() {
            return this.dateWithTemporal;
        }

        public void setDateWithTemporal(Date dateWithTemporal) {
            this.dateWithTemporal = dateWithTemporal;
        }

        @Temporal(TemporalType.TIME)
        @Column(name = "time_with_temporal")
        public Date getTimeWithTemporal() {
            return this.timeWithTemporal;
        }

        public void setTimeWithTemporal(Date timeWithTemporal) {
            this.timeWithTemporal = timeWithTemporal;
        }

        @Temporal(TemporalType.TIMESTAMP)
        @Column(name = "timestamp_with_temporal")
        public Date getTimestampWithTemporal() {
            return this.timestampWithTemporal;
        }

        public void setTimestampWithTemporal(Date timestampWithTemporal) {
            this.timestampWithTemporal = timestampWithTemporal;
        }
    }
}
