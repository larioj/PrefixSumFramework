import java.nio.ByteBuffer

import com.google.protobuf.ByteString

val x = 22
val y = 33

val data = ByteString.copyFrom(ByteBuffer.allocate(8).putInt(0, x).putInt(4, y))

val xp = data.asReadOnlyByteBuffer().getInt(0)
val yp = data.asReadOnlyByteBuffer().getInt(4)

