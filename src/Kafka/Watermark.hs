module Kafka.Watermark(
  queryWatermarkOffsets
  , getWatermarkOffsets
  ) where

import Kafka.Internal.RdKafka
import Kafka.Internal.RdKafkaEnum
import Kafka.Types
import Kafka.Consumer.Types

-- ^ Query the broker the watermarks, the first and last offsets of the given topic partition.
-- This function is very slow since it communicates with brokers.  Use 'getWatermarkOffsets' if you are
-- Ok with the watermarks stored in the client.      
queryWatermarkOffsets 
  :: RdKafkaTPtr                             -- ^ Kafka handle
  -> TopicName                               -- ^ topic
  -> PartitionId                             -- ^ partition
  -> Timeout                                 -- ^ timeout in milli secs
  -> IO (Either KafkaError (Offset, Offset)) -- ^ error, or the low and high watermarks
queryWatermarkOffsets k (TopicName topic) (PartitionId partition) (Timeout tout) = do
  (r, x, y) <- rdKafkaQueryWatermarkOffsets k topic (fromIntegral partition) tout
  return $ case r of
    RdKafkaRespErrNoError -> Right (Offset $ fromIntegral x, Offset $ fromIntegral y)
    e -> Left $ KafkaResponseError e

-- ^ Get the broker the watermarks, the first and last offsets of the given topic partition, stored in the client memory.
getWatermarkOffsets 
  :: RdKafkaTPtr                             -- ^ Kafka handle
  -> TopicName                               -- ^ topic
  -> PartitionId                             -- ^ partition
  -> IO (Either KafkaError (Offset, Offset)) -- ^ error, or the low and high watermarks
getWatermarkOffsets k (TopicName topic) (PartitionId partition) = do
  (r, x, y) <- rdKafkaGetWatermarkOffsets k topic (fromIntegral partition)
  return $ case r of
    RdKafkaRespErrNoError -> Right (Offset $ fromIntegral x, Offset $ fromIntegral y)
    e -> Left $ KafkaResponseError e
