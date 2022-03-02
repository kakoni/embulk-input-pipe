package org.embulk.input.pipe;

import java.io.IOException;
import java.io.InputStream;

public class EofInputStream extends InputStream {

    protected InputStream wrappedStream;
    private boolean selfClosed;

    public EofInputStream(final InputStream in)
    {
        wrappedStream = in;
        selfClosed = false;
    }

    protected boolean isReadAllowed() throws IOException {
        if (selfClosed) {
            throw new IOException("Attempted read on closed stream.");
        }
        return (wrappedStream != null);
    }

    @Override
    public int read() throws IOException {
        int l = -1;

        if (isReadAllowed()) {
            try {
                l = wrappedStream.read();
                checkEOF(l);
            } catch (final IOException ex) {
                checkAbort();
                throw ex;
            }
        }

        return l;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        int l = -1;

        if (isReadAllowed()) {
            try {
                l = wrappedStream.read(b,  off,  len);
                checkEOF(l);
            } catch (final IOException ex) {
                checkAbort();
                throw ex;
            }
        }

        return l;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int available() throws IOException {
        int a = 0; // not -1

        if (isReadAllowed()) {
            try {
                a = wrappedStream.available();
                // no checkEOF() here, available() can't trigger EOF
            } catch (final IOException ex) {
                checkAbort();
                throw ex;
            }
        }

        return a;
    }

    @Override
    public void close() throws IOException {
        // tolerate multiple calls to close()
        selfClosed = true;
        checkClose();
    }

    protected void checkEOF(final int eof) throws IOException {
        if ((wrappedStream != null) && (eof < 0)) {
            try {
                boolean scws = true; // should close wrapped stream?
                if (scws) {
                    wrappedStream.close();
                }
            } finally {
                wrappedStream = null;
            }
        }
    }

    protected void checkClose() throws IOException {

        if (wrappedStream != null) {
            try {
                boolean scws = true; // should close wrapped stream?
                if (scws) {
                    wrappedStream.close();
                }
            } finally {
                wrappedStream = null;
            }
        }
    }

    protected void checkAbort() throws IOException {

        if (wrappedStream != null) {
            try {
                boolean scws = true; // should close wrapped stream?
                if (scws) {
                    wrappedStream.close();
                }
            } finally {
                wrappedStream = null;
            }
        }
    }

}