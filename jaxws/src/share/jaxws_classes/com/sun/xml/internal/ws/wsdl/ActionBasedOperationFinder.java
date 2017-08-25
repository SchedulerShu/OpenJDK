/*
 * Copyright (c) 1997, 2010, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.sun.xml.internal.ws.wsdl;

import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;
import com.sun.xml.internal.ws.api.WSBinding;
import com.sun.xml.internal.ws.api.model.SEIModel;
import com.sun.xml.internal.ws.api.model.wsdl.WSDLPort;
import com.sun.xml.internal.ws.api.model.wsdl.WSDLBoundOperation;
import com.sun.xml.internal.ws.api.addressing.AddressingVersion;
import com.sun.xml.internal.ws.api.message.HeaderList;
import com.sun.xml.internal.ws.api.message.Message;
import com.sun.xml.internal.ws.api.message.Messages;
import com.sun.xml.internal.ws.api.message.Packet;
import com.sun.xml.internal.ws.model.AbstractSEIModelImpl;
import com.sun.xml.internal.ws.model.JavaMethodImpl;
import static com.sun.xml.internal.ws.wsdl.PayloadQNameBasedOperationFinder.*;
import com.sun.xml.internal.ws.resources.AddressingMessages;

import javax.xml.namespace.QName;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * An {@link WSDLOperationFinder} implementation that uses
 * WS-Addressing Action Message Addressing Property, <code>wsa:Action</code> and SOAP Payload QName,
 * as the key for dispatching.
 * <p/>
 * This should be used only when AddressingFeature is enabled.
 * A map of all {@link ActionBasedOperationSignature}s in the port and the corresponding and the WSDL Operation QNames
 * is maintained.
 * <p/>
 *
 * @author Rama Pulavarthi
 */
final class ActionBasedOperationFinder extends WSDLOperationFinder {

    private static final Logger LOGGER = Logger.getLogger(ActionBasedOperationFinder.class.getName());
    private final Map<ActionBasedOperationSignature, QName> uniqueOpSignatureMap;
    private final Map<String, QName> actionMap;

    private final @NotNull AddressingVersion av;

    public ActionBasedOperationFinder(WSDLPort wsdlModel, WSBinding binding, @Nullable SEIModel seiModel) {
        super(wsdlModel, binding, seiModel);

        assert binding.getAddressingVersion() != null;    // this dispatcher can be only used when addressing is on.
        av = binding.getAddressingVersion();
        uniqueOpSignatureMap = new HashMap<ActionBasedOperationSignature, QName>();
        actionMap = new HashMap<String,QName>();

        if (seiModel != null) {
            for (JavaMethodImpl m : ((AbstractSEIModelImpl) seiModel).getJavaMethods()) {
                if(m.getMEP().isAsync)
                    continue;

                String action = m.getInputAction();
                QName payloadName = m.getRequestPayloadName();
                if (payloadName == null)
                    payloadName = EMPTY_PAYLOAD;
                //first look at annotations and then in wsdlmodel
                if (action == null || action.equals("")) {
                    action = m.getOperation().getOperation().getInput().getAction();

                }
                if (action != null) {
                    ActionBasedOperationSignature opSignature = new ActionBasedOperationSignature(action, payloadName);
                    if(uniqueOpSignatureMap.get(opSignature) != null) {
                        LOGGER.warning(AddressingMessages.NON_UNIQUE_OPERATION_SIGNATURE(
                                uniqueOpSignatureMap.get(opSignature),m.getOperation().getName(),action,payloadName));
                    }
                    uniqueOpSignatureMap.put(opSignature, m.getOperation().getName());
                    actionMap.put(action,m.getOperation().getName());
                }
            }
        } else {
            for (WSDLBoundOperation wsdlOp : wsdlModel.getBinding().getBindingOperations()) {
                QName payloadName = wsdlOp.getReqPayloadName();
                if (payloadName == null)
                    payloadName = EMPTY_PAYLOAD;
                String action = wsdlOp.getOperation().getInput().getAction();
                ActionBasedOperationSignature opSignature = new ActionBasedOperationSignature(
                        action, payloadName);
                if(uniqueOpSignatureMap.get(opSignature) != null) {
                    LOGGER.warning(AddressingMessages.NON_UNIQUE_OPERATION_SIGNATURE(
                                    uniqueOpSignatureMap.get(opSignature),wsdlOp.getName(),action,payloadName));

                }
                uniqueOpSignatureMap.put(opSignature, wsdlOp.getName());
                actionMap.put(action,wsdlOp.getName());
            }
        }
    }

    /**
     *
     * @param request  Request Packet that is used to find the associated WSDLOperation
     * @return WSDL operation Qname.
     *         return null if WS-Addressing is not engaged.
     * @throws DispatchException with WSA defined fault message when it cannot find an associated WSDL operation.
     *
     */
    @Override
    public QName getWSDLOperationQName(Packet request) throws DispatchException {
        HeaderList hl = request.getMessage().getHeaders();
        String action = hl.getAction(av, binding.getSOAPVersion());

        if (action == null)
            // Addressing is not enagaged, return null to use other ways to dispatch.
            return null;

        Message message = request.getMessage();
        QName payloadName;
        String localPart = message.getPayloadLocalPart();
        if (localPart == null) {
            payloadName = EMPTY_PAYLOAD;
        } else {
            String nsUri = message.getPayloadNamespaceURI();
            if (nsUri == null)
                nsUri = EMPTY_PAYLOAD_NSURI;
            payloadName = new QName(nsUri, localPart);
        }

        QName opName = uniqueOpSignatureMap.get(new ActionBasedOperationSignature(action, payloadName));
        if (opName != null)
            return opName;

        //Seems like in Wstrust STS wsdls, the payload does not match what is specified in the wsdl leading to incorrect
        //  wsdl operation resolution. Use just wsa:Action to dispatch as a last resort.
        //try just with wsa:Action
        opName = actionMap.get(action);
        if (opName != null)
            return opName;

        // invalid action header
        Message result = Messages.create(action, av, binding.getSOAPVersion());

        throw new DispatchException(result);

    }


}
