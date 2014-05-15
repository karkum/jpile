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
        String contents = "Because I've found that EVERYBODY gets a \"You use _% more power than everyone else in the " +
                "state\" " +
                "rating.  Everyone else on my street uses gas, oil, or wood for heat.  I use electric.  " +
                "\"You used 20% MORE electricity than my neighbors.  This costs you about  $279 EXTRA per year.\"  " +
                "WOW!  I'm paying $279 EXTRA per year to heat my house at a higher temperature than a comparible neighbor's " +
                "house who has paid over $1460 (so far) for fuel oil THIS HEATING SEASON!  They're jealous!\r\n" +
                "\r\n" +
                "Someone in your advertising department needs to get a clue, because it's making your company look like it's " +
                "being run by idiots.  I'll reconsider receiving these reports when they are mailed out on softer paper and " +
                "flushable.   At that point they'll have a valid use.";
        this.infileDataBuffer.append(contents);
        addRowAndAssertContents(contents);
    }

    @Test
    public void testAppendBadString() {
        String contents = "Gladiator\tis the best movie ever!\r";
        this.infileDataBuffer.append(contents);
        addRowAndAssertContents(contents);
    }

    @Test
    public void testAppendStringNeedingEscaping() {
        String contents = "C:\\windows\\bluescreen.png";
        this.infileDataBuffer.append(contents);
        addRowAndAssertContents(contents.replace("\\", "\\\\"));
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

    private void addRowAndAssertContents(String expected) {
        try {
            this.infileDataBuffer.addRowToInfile();
            assertEquals(expected, CharStreams.toString(new InputStreamReader(infileDataBuffer.asInputStream())));
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
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        @Temporal(TemporalType.DATE)
        @Column(name = "date_with_temporal")
        public Date getDateWithTemporal() {
            return dateWithTemporal;
        }

        public void setDateWithTemporal(Date dateWithTemporal) {
            this.dateWithTemporal = dateWithTemporal;
        }

        @Temporal(TemporalType.TIME)
        @Column(name = "time_with_temporal")
        public Date getTimeWithTemporal() {
            return timeWithTemporal;
        }

        public void setTimeWithTemporal(Date timeWithTemporal) {
            this.timeWithTemporal = timeWithTemporal;
        }

        @Temporal(TemporalType.TIMESTAMP)
        @Column(name = "timestamp_with_temporal")
        public Date getTimestampWithTemporal() {
            return timestampWithTemporal;
        }

        public void setTimestampWithTemporal(Date timestampWithTemporal) {
            this.timestampWithTemporal = timestampWithTemporal;
        }
    }
}
