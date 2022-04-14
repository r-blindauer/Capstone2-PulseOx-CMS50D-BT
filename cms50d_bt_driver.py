"""
Async callbacks with a queue and external consumer

"""
import sys
import time
import platform
import asyncio
import logging

output_file = open(
    "C:\\Users\\blind\\Documents\\College\Computer Science\\Capstone1\\DataConnection\\PulseOx_Output.txt",
    'w')

# CMS50D-BT Message Protocol format
CMS_LIVEDATA_PACKET_HEADER = 235  # 0xEB
CMS_PULSE_WAVE_MSG_ID = 0  # 0x00
CMS_HR_SPO2_MSG_ID = 1  # 0x01
CMS_PULSE_WAVE_MSG_LENGTH = 6
CMS_HR_SPO2_MSG_LENGTH = 8

from bleak import BleakClient

logger = logging.getLogger(__name__)

ADDRESS = (
    "C2:99:34:C4:A2:00"
)

running_flag = True
msg_buffer = bytearray()
msg_decoded = bytearray()


async def run_ble_client(address: str, queue: asyncio.Queue):
    global running_flag

    async def callback_handler(sender, data):
        await queue.put((time.time(), data))

    # added timeout
    async with BleakClient(address) as client:
        logger.info(f"Connected: {client.is_connected}")
        await client.start_notify("0000ff02-0000-1000-8000-00805f9b34fb", callback_handler)
        # await client.start_notify("0000ff03-0000-1000-8000-00805f9b34fb", callback_handler) # changed ff04 to ff03

        await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x81\x01'), response=False)
        await asyncio.sleep(0.5)
        await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x82\x02'), response=False)
        await asyncio.sleep(0.5)
        await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x9a\x1a'), response=False)
        await asyncio.sleep(0.5)
        await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x9b\x00\x1b'), response=False)
        await asyncio.sleep(1.0)
        await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x9b\x01\x1c'), response=False)

        # wait for 30s
        while True == running_flag:
            await asyncio.sleep(5.0)
            await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x9a\x1a'), response=False)

        # Stop streaming data
        await client.write_gatt_char("0000ff01-0000-1000-8000-00805f9b34fb", bytearray(b'\x9b\x7f\x1a'), response=False)

        # Send an "exit command to the consumer"
        await queue.put((time.time(), None))


def process_livedata(epoch, data):
    global msg_buffer

    msg_buffer += data

    # Handle the case, partial message received in the livedata stream
    i = 0

    # Create output text file
    f = open("PulseOx_Output.txt", "w")

    while i < len(msg_buffer):
        msg_decoded = bytearray()
        # New message decoded
        if CMS_LIVEDATA_PACKET_HEADER == msg_buffer[i]:
            if CMS_PULSE_WAVE_MSG_ID == msg_buffer[i + 1] and CMS_PULSE_WAVE_MSG_LENGTH <= len(msg_buffer):
                msg_decoded = msg_buffer[i:CMS_PULSE_WAVE_MSG_LENGTH]
                # Pop out the message from the msg buffer
                msg_buffer = msg_buffer[CMS_PULSE_WAVE_MSG_LENGTH:]
                i = 0  # Reset counter since message popped out
            elif CMS_HR_SPO2_MSG_ID == msg_buffer[i + 1] and CMS_HR_SPO2_MSG_LENGTH <= len(msg_buffer):
                msg_decoded = msg_buffer[i:CMS_HR_SPO2_MSG_LENGTH]
                # Pop out the message from the msg buffer
                msg_buffer = msg_buffer[CMS_HR_SPO2_MSG_LENGTH:]
                i = 0  # Reset counter since message popped out
            # Check if one message has been successfully decoded
            if 0 < len(msg_decoded):
                msg = list(msg_decoded)
                if 0 == msg[1]:
                    pass
                else:
                    # Only print the valid SpO2 readings
                    spo2_val = msg[4]
                    # Only valid readings
                    if spo2_val > 50 and spo2_val < 100:
                        output_file.write(str(spo2_val) + "\n")
                        print('Time:', str(epoch), 'SpO2:', spo2_val)
            else:
                # Step out loop since no complete message remaining in the buffer
                break
        else:
            # pop out the element
            msg_buffer = msg_buffer[i + 1:]


async def run_queue_consumer(queue: asyncio.Queue):
    global running_flag

    t_end = time.time() + 20
    #while True:
    while time.time() < t_end:
        # Use await asyncio.wait_for(queue.get(), timeout=1.0) if you want a timeout for getting data.
        epoch, data = await queue.get()
        #epoch, data = await asyncio.wait_for(queue.get(), timeout-self.)

        if data is None:
            logger.info(
                "Got message from client about disconnection. Exiting consumer loop..."
            )
            break
        else:
            logger.info(f"Received callback data via async queue at {epoch}: {data}")
            process_livedata(epoch, data)

    print("Done collecting data")
    output_file.close()
    running_flag = False


async def main(address: str):
    queue = asyncio.Queue()
    # Connect to BL device
    client_task = run_ble_client(address, queue)
    # Get information
    consumer_task = run_queue_consumer(queue)
    # await asyncio.wait_for(asyncio.gather(client_task, consumer_task)
    await asyncio.gather(client_task, consumer_task)
    print("main method done")
    logger.info("Main method done.")


if __name__ == "__main__":
    # To have logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(
        main(
            sys.argv[1] if len(sys.argv) > 1 else ADDRESS
        )
    )
