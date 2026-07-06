#pragma once

#include "PerfMessagePubSubTypes.h"

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

namespace dperf
{

constexpr const char* kTypeName = "PerfMessage";
constexpr const char* kRequestTopic = "PerfMessageRequest";
constexpr const char* kReplyTopic = "PerfMessageReply";
constexpr size_t kMaxPayloadSize = 64 * 1024 * 1024;

enum class Reliability
{
    BestEffort,
    Reliable,
};

struct Options
{
    size_t size = 1024;
    Reliability reliability = Reliability::BestEffort;
    uint32_t domain = 0;
    uint64_t samples = 0;
    uint32_t interval_us = 0;
    uint32_t warmup_ms = 1000;
};

inline uint64_t now_ns()
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

inline uint64_t parse_u64(const char* value, const char* name)
{
    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0')
    {
        throw std::invalid_argument(std::string("invalid ") + name + ": " + value);
    }
    return parsed;
}

inline Reliability parse_reliability(const std::string& value)
{
    if (value == "best_effort" || value == "besteffort" || value == "best-effort")
    {
        return Reliability::BestEffort;
    }
    if (value == "reliable")
    {
        return Reliability::Reliable;
    }
    throw std::invalid_argument("qos must be best_effort or reliable");
}

inline void print_common_usage(const char* program, bool publisher)
{
    std::cerr
        << "Usage: " << program << " --size BYTES --qos best_effort|reliable [options]\n"
        << "Options:\n"
        << "  --domain ID          DDS domain id (default: 0)\n";
    if (publisher)
    {
        std::cerr
            << "  --samples N          stop after N sent samples (default: 0, infinite)\n"
            << "  --interval-us N      delay between sends (default: 0)\n"
            << "  --warmup-ms N        wait before publishing for DDS discovery (default: 1000)\n";
    }
}

inline Options parse_options(int argc, char** argv, bool publisher)
{
    Options options;
    for (int i = 1; i < argc; ++i)
    {
        const std::string arg(argv[i]);
        auto require_value = [&](const char* name) -> const char*
        {
            if (i + 1 >= argc)
            {
                throw std::invalid_argument(std::string("missing value for ") + name);
            }
            return argv[++i];
        };

        if (arg == "--size" || arg == "-s")
        {
            options.size = static_cast<size_t>(parse_u64(require_value(arg.c_str()), "size"));
        }
        else if (arg == "--qos" || arg == "-q")
        {
            options.reliability = parse_reliability(require_value(arg.c_str()));
        }
        else if (arg == "--domain" || arg == "-d")
        {
            options.domain = static_cast<uint32_t>(parse_u64(require_value(arg.c_str()), "domain"));
        }
        else if (publisher && (arg == "--samples" || arg == "-n"))
        {
            options.samples = parse_u64(require_value(arg.c_str()), "samples");
        }
        else if (publisher && arg == "--interval-us")
        {
            options.interval_us = static_cast<uint32_t>(parse_u64(require_value(arg.c_str()), "interval-us"));
        }
        else if (publisher && arg == "--warmup-ms")
        {
            options.warmup_ms = static_cast<uint32_t>(parse_u64(require_value(arg.c_str()), "warmup-ms"));
        }
        else if (arg == "--help" || arg == "-h")
        {
            print_common_usage(argv[0], publisher);
            std::exit(0);
        }
        else
        {
            throw std::invalid_argument("unknown option: " + arg);
        }
    }

    if (options.size > kMaxPayloadSize)
    {
        throw std::invalid_argument("size exceeds maximum supported payload of 67108864 bytes");
    }
    return options;
}

inline void apply_writer_qos(eprosima::fastdds::dds::DataWriterQos& qos, Reliability reliability)
{
    using namespace eprosima::fastdds::dds;
    qos.reliability().kind = reliability == Reliability::Reliable
            ? RELIABLE_RELIABILITY_QOS
            : BEST_EFFORT_RELIABILITY_QOS;
    qos.durability().kind = VOLATILE_DURABILITY_QOS;
    qos.history().kind = KEEP_LAST_HISTORY_QOS;
    qos.history().depth = 1024;
    qos.resource_limits().max_samples = 2048;
    qos.resource_limits().allocated_samples = 128;
    qos.publish_mode().kind = SYNCHRONOUS_PUBLISH_MODE;
}

inline void apply_reader_qos(eprosima::fastdds::dds::DataReaderQos& qos, Reliability reliability)
{
    using namespace eprosima::fastdds::dds;
    qos.reliability().kind = reliability == Reliability::Reliable
            ? RELIABLE_RELIABILITY_QOS
            : BEST_EFFORT_RELIABILITY_QOS;
    qos.durability().kind = VOLATILE_DURABILITY_QOS;
    qos.history().kind = KEEP_LAST_HISTORY_QOS;
    qos.history().depth = 1024;
    qos.resource_limits().max_samples = 2048;
    qos.resource_limits().allocated_samples = 128;
}

class DdsEntities
{
public:
    explicit DdsEntities(uint32_t domain)
        : factory_(eprosima::fastdds::dds::DomainParticipantFactory::get_instance())
        , type_(new PerfMessagePubSubType())
    {
        using namespace eprosima::fastdds::dds;

        participant_ = factory_->create_participant(domain, PARTICIPANT_QOS_DEFAULT);
        if (participant_ == nullptr)
        {
            throw std::runtime_error("failed to create DDS participant");
        }
        if (participant_->register_type(type_) != eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK)
        {
            throw std::runtime_error("failed to register PerfMessage type");
        }
        publisher_ = participant_->create_publisher(PUBLISHER_QOS_DEFAULT);
        subscriber_ = participant_->create_subscriber(SUBSCRIBER_QOS_DEFAULT);
        if (publisher_ == nullptr || subscriber_ == nullptr)
        {
            throw std::runtime_error("failed to create DDS publisher/subscriber");
        }
    }

    ~DdsEntities()
    {
        if (participant_ != nullptr)
        {
            participant_->delete_contained_entities();
            factory_->delete_participant(participant_);
        }
    }

    eprosima::fastdds::dds::Topic* topic(const char* name)
    {
        using namespace eprosima::fastdds::dds;
        Topic* t = participant_->create_topic(name, kTypeName, TOPIC_QOS_DEFAULT);
        if (t == nullptr)
        {
            throw std::runtime_error(std::string("failed to create topic ") + name);
        }
        return t;
    }

    eprosima::fastdds::dds::DataWriter* writer(eprosima::fastdds::dds::Topic* topic, Reliability reliability)
    {
        eprosima::fastdds::dds::DataWriterQos qos;
        publisher_->get_default_datawriter_qos(qos);
        apply_writer_qos(qos, reliability);
        auto* writer = publisher_->create_datawriter(topic, qos);
        if (writer == nullptr)
        {
            throw std::runtime_error("failed to create DDS writer");
        }
        return writer;
    }

    eprosima::fastdds::dds::DataReader* reader(eprosima::fastdds::dds::Topic* topic, Reliability reliability)
    {
        eprosima::fastdds::dds::DataReaderQos qos;
        subscriber_->get_default_datareader_qos(qos);
        apply_reader_qos(qos, reliability);
        auto* reader = subscriber_->create_datareader(topic, qos);
        if (reader == nullptr)
        {
            throw std::runtime_error("failed to create DDS reader");
        }
        return reader;
    }

private:
    eprosima::fastdds::dds::DomainParticipantFactory* factory_ = nullptr;
    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    eprosima::fastdds::dds::Publisher* publisher_ = nullptr;
    eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;
    eprosima::fastdds::dds::TypeSupport type_;
};

inline void tiny_sleep()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

} // namespace dperf
