/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import rice.pastry.Id;

public class PastryIdFactory implements IdFactory {
	public Id getIdFromContent(byte[] material) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(material);
			return Id.build(md.digest());
		} catch (NoSuchAlgorithmException nsae) {
			throw new RuntimeException("Couldn't get SHA-1 digester!");
		}
	}

	public Id getIdFromByteArray(byte[] bytes) {
		return Id.build(bytes);
	}
	
}