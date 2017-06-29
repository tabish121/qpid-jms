/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp.message;

import java.util.Date;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;

/**
 * Wraps around the proton Properties object and provides an ability to
 * determine if the Properties can be optimized out of message encodes
 */
public class AmqpProperties implements Section {

    private static int MESSAGE_ID = 1;
    private static int USER_ID = 2;
    private static int TO = 4;
    private static int SUBJECT = 8;
    private static int REPLY_TO = 16;
    private static int CORRELATION_ID = 32;
    private static int CONTENT_TYPE = 64;
    private static int CONTENT_ENCODING = 128;
    private static int ABSOLUTE_EXPIRYTIME = 256;
    private static int CREATION_TIME = 512;
    private static int GROUP_ID = 1024;
    private static int GROUP_SEQUENCE = 2048;
    private static int REPLY_TO_GROUP_ID = 4096;

    private int modified = 0;

    private Object messageId;
    private Binary userId;
    private String to;
    private String subject;
    private String replyTo;
    private Object correlationId;
    private Symbol contentType;
    private Symbol contentEncoding;
    private Long absoluteExpiryTime;
    private Long creationTime;
    private String groupId;
    private UnsignedInteger groupSequence;
    private String replyToGroupId;

    public AmqpProperties() {
    }

    public AmqpProperties(AmqpProperties other) {
        setProperties(other);
    }

    public void setProperties(Properties other) {
        if (other != null) {
            setMessageId(other.getMessageId());
            setUserId(other.getUserId());
            setTo(other.getTo());
            setSubject(other.getSubject());
            setReplyTo(other.getReplyTo());
            setCorrelationId(other.getCorrelationId());
            setContentType(other.getContentType());
            setContentEncoding(other.getContentEncoding());
            setAbsoluteExpiryTime(other.getAbsoluteExpiryTime() == null ? 0 : other.getAbsoluteExpiryTime().getTime());
            setCreationTime(other.getCreationTime() == null ? 0 : other.getCreationTime().getTime());
            setGroupId(other.getGroupId());
            setGroupSequence(other.getGroupSequence());
            setReplyToGroupId(other.getReplyToGroupId());
        }
    }

    public void setProperties(AmqpProperties other) {
        if (other != null) {
            this.messageId = other.messageId;
            this.userId = other.userId;
            this.to = other.to;
            this.subject = other.subject;
            this.replyTo = other.replyTo;
            this.correlationId = other.correlationId;
            this.contentType = other.contentType;
            this.contentEncoding = other.contentEncoding;
            this.absoluteExpiryTime = other.absoluteExpiryTime;
            this.creationTime = other.creationTime;
            this.groupId = other.groupId;
            this.groupSequence = other.groupSequence;
            this.replyToGroupId = other.replyToGroupId;
        }
    }

    public Properties getProperties() {
        Properties result = null;

        if (!isDefault()) {
            result = new Properties();
            result.setMessageId(messageId);
            result.setUserId(userId);
            result.setTo(to);
            result.setSubject(subject);
            result.setReplyTo(replyTo);
            result.setCorrelationId(correlationId);
            result.setContentType(contentType);
            result.setContentEncoding(contentEncoding);
            result.setAbsoluteExpiryTime(absoluteExpiryTime == null ? null : new Date(absoluteExpiryTime));
            result.setCreationTime(creationTime == null ? null : new Date(creationTime));
            result.setGroupId(groupId);
            result.setGroupSequence(groupSequence);
            result.setReplyToGroupId(replyToGroupId);
        }

        return result;
    }

    //----- Query the state of the Header object -----------------------------//

    public boolean isDefault() {
        return modified == 0;
    }

    public int getModifiedFlag() {
        return modified;
    }

    public boolean nonDefaultMessageId() {
        return (modified & MESSAGE_ID) == MESSAGE_ID;
    }

    public boolean nonDefaultUserId() {
        return (modified & USER_ID) == USER_ID;
    }

    public boolean nonDefaultTo() {
        return (modified & TO) == TO;
    }

    public boolean nonDefaultSubject() {
        return (modified & SUBJECT) == SUBJECT;
    }

    public boolean nonDefaultReplyTo() {
        return (modified & REPLY_TO) == REPLY_TO;
    }

    public boolean nonDefaultCorrelationId() {
        return (modified & CORRELATION_ID) == CORRELATION_ID;
    }

    public boolean nonDefaultContentType() {
        return (modified & CONTENT_TYPE) == CONTENT_TYPE;
    }

    public boolean nonDefaultContentEncoding() {
        return (modified & CONTENT_ENCODING) == CONTENT_ENCODING;
    }

    public boolean nonDefaultAbsoluteExpiryTime() {
        return (modified & ABSOLUTE_EXPIRYTIME) == ABSOLUTE_EXPIRYTIME;
    }

    public boolean nonDefaultCreationTime() {
        return (modified & CREATION_TIME) == CREATION_TIME;
    }

    public boolean nonDefaultGroupId() {
        return (modified & GROUP_ID) == GROUP_ID;
    }

    public boolean nonDefaultGroupSequence() {
        return (modified & GROUP_SEQUENCE) == GROUP_SEQUENCE;
    }

    public boolean nonDefaultReplyToGroupId() {
        return (modified & REPLY_TO_GROUP_ID) == REPLY_TO_GROUP_ID;
    }

    //----- Access the AMQP Properties object ------------------------------------//

    public Object getMessageId() {
        return messageId;
    }

    public void setMessageId(Object value) {
        if (value == null) {
            modified &= ~MESSAGE_ID;
            messageId = null;
        } else {
            modified |= MESSAGE_ID;
            messageId = value;
        }
    }

    public Binary getUserId() {
        return userId;
    }

    public void setUserId(Binary value) {
        if (value == null || value.getArray() == null) {
            modified &= ~USER_ID;
            userId = null;
        } else {
            modified |= USER_ID;
            userId = value;
        }
    }

    public String getTo() {
        return to;
    }

    public void setTo(String value) {
        if (value == null || value.isEmpty()) {
            modified &= ~TO;
            to = null;
        } else {
            modified |= TO;
            to = value;
        }
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String value) {
        if (value == null || value.isEmpty()) {
            modified &= ~SUBJECT;
            subject = null;
        } else {
            modified |= SUBJECT;
            subject = value;
        }
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String value) {
        if (value == null || value.isEmpty()) {
            modified &= ~REPLY_TO;
            replyTo = null;
        } else {
            modified |= REPLY_TO;
            replyTo = value;
        }
    }

    public Object getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(Object value) {
        if (value == null) {
            modified &= ~CORRELATION_ID;
            correlationId = null;
        } else {
            modified |= CORRELATION_ID;
            correlationId = value;
        }
    }

    public String getContentType() {
        return contentType == null ? null : contentType.toString();
    }

    public Symbol getRawContentType() {
        return contentType;
    }

    public void setContentType(Symbol value) {
        if (value == null) {
            modified &= ~CONTENT_TYPE;
            contentType = null;
        } else {
            modified |= CONTENT_TYPE;
            contentType = value;
        }
    }

    public String getContentEncoding() {
        return contentEncoding == null ? null : contentEncoding.toString();
    }

    public Symbol getRawContentEncoding() {
        return contentEncoding;
    }

    public void setContentEncoding(Symbol value) {
        if (value == null) {
            modified &= ~CONTENT_ENCODING;
            contentEncoding = null;
        } else {
            modified |= CONTENT_ENCODING;
            contentEncoding = value;
        }
    }

    public long getAbsoluteExpiryTime() {
        return absoluteExpiryTime == null ? 0l : absoluteExpiryTime;
    }

    public Long getRawAbsoluteExpiryTime() {
        return absoluteExpiryTime;
    }

    public void setAbsoluteExpiryTime(Long value) {
        if (value == null) {
            modified &= ~ABSOLUTE_EXPIRYTIME;
            absoluteExpiryTime = null;
        } else {
            modified |= ABSOLUTE_EXPIRYTIME;
            absoluteExpiryTime = Long.valueOf(value);
        }
    }

    public void setAbsoluteExpiryTime(long value) {
        if (value == 0) {
            setAbsoluteExpiryTime(null);
        } else {
            setAbsoluteExpiryTime(Long.valueOf(value));
        }
    }

    public long getCreationTime() {
        return creationTime == null ? 0l : creationTime;
    }

    public Long getRawCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Long value) {
        if (value == null) {
            modified &= ~CREATION_TIME;
            creationTime = null;
        } else {
            modified |= CREATION_TIME;
            creationTime = value;
        }
    }

    public void setCreationTime(long value) {
        if (value == 0) {
            setCreationTime(null);
        } else {
            setCreationTime(Long.valueOf(value));
        }
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String value) {
        if (value == null || value.isEmpty()) {
            modified &= ~GROUP_ID;
            groupId = null;
        } else {
            modified |= GROUP_ID;
            groupId = value;
        }
    }

    public long getGroupSequence() {
        return groupSequence == null ? 0l : groupSequence.longValue();
    }

    public UnsignedInteger getRawGroupSequence() {
        return groupSequence;
    }

    public void setGroupSequence(UnsignedInteger value) {
        if (value == null) {
            modified &= ~GROUP_SEQUENCE;
            groupSequence = null;
        } else {
            modified |= GROUP_SEQUENCE;
            groupSequence = value;
        }
    }

    public void setGroupSequence(long value) {
        if (value == 0) {
            setGroupSequence(null);
        } else {
            setGroupSequence(UnsignedInteger.valueOf(value));
        }
    }

    public String getReplyToGroupId() {
        return replyToGroupId;
    }

    public void setReplyToGroupId(String value) {
        if (value == null || value.isEmpty()) {
            modified &= ~REPLY_TO_GROUP_ID;
            replyToGroupId = null;
        } else {
            modified |= REPLY_TO_GROUP_ID;
            replyToGroupId = value;
        }
    }

    @Override
    public String toString() {
        return "AmqpProperties {" +
               "messageId=" + messageId +
               ", userId=" + userId +
               ", to='" + to + '\'' +
               ", subject='" + subject + '\'' +
               ", replyTo='" + replyTo + '\'' +
               ", correlationId=" + correlationId +
               ", contentType=" + contentType +
               ", contentEncoding=" + contentEncoding +
               ", absoluteExpiryTime=" + absoluteExpiryTime +
               ", creationTime=" + creationTime +
               ", groupId='" + groupId + '\'' +
               ", groupSequence=" + groupSequence +
               ", replyToGroupId='" + replyToGroupId + '\'' +
               " }";
    }
}
