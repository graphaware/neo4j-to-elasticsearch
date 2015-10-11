CREATE CONSTRAINT ON (person:Person) ASSERT person.neogen_id IS UNIQUE;
CREATE CONSTRAINT ON (skill:Skill) ASSERT skill.neogen_id IS UNIQUE;
CREATE CONSTRAINT ON (company:Company) ASSERT company.neogen_id IS UNIQUE;
CREATE CONSTRAINT ON (country:Country) ASSERT country.neogen_id IS UNIQUE;
MERGE (n1:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9' })
SET n1.firstname = 'Kelly', n1.lastname = 'Krajcik';
MERGE (n2:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f' })
SET n2.firstname = 'Janick', n2.lastname = 'Gulgowski';
MERGE (n3:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868' })
SET n3.firstname = 'Mac', n3.lastname = 'Kozey';
MERGE (n4:Person {neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3' })
SET n4.firstname = 'Alberto', n4.lastname = 'Zieme';
MERGE (n5:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533' })
SET n5.firstname = 'Sammie', n5.lastname = 'Emmerich';
MERGE (n6:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f' })
SET n6.firstname = 'Aylin', n6.lastname = 'Kling';
MERGE (n7:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af' })
SET n7.firstname = 'Chasity', n7.lastname = 'Lakin';
MERGE (n8:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939' })
SET n8.firstname = 'Shanelle', n8.lastname = 'Schoen';
MERGE (n9:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d' })
SET n9.firstname = 'Favian', n9.lastname = 'Christiansen';
MERGE (n10:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226' })
SET n10.firstname = 'Ashtyn', n10.lastname = 'Lehner';
MERGE (n11:Skill {neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a' })
SET n11.name = 'WebQL';
MERGE (n12:Skill {neogen_id: '0763e4e108132acc614319b63511247d06555da5' })
SET n12.name = 'LSL';
MERGE (n13:Skill {neogen_id: '59fee4dd0cf9395752c075b4965ac59f40246a66' })
SET n13.name = 'Deesel (formerly G)';
MERGE (n14:Skill {neogen_id: 'b6415157d0146fbdf85d631ab9bd4864c6826549' })
SET n14.name = 'ALGOL 58';
MERGE (n15:Skill {neogen_id: 'fe6c7a6df60e3e43939a659a958ba1d8e11d0a7b' })
SET n15.name = 'JScript .NET';
MERGE (n16:Company {neogen_id: 'f80427bf92a435114bbf8ba199fa1eee15e05da6' })
SET n16.name = 'Upton PLC', n16.desc = 'Diverse optimizing access';
MERGE (n17:Company {neogen_id: '020a8dc29f1f667b2facd86e9dc8aab54bc3fc13' })
SET n17.name = 'Lakin Inc', n17.desc = 'Managed impactful strategy';
MERGE (n18:Company {neogen_id: '8d2774b8d3445fda288d9d5e7490a3069a85802c' })
SET n18.name = 'Lockman-Schumm', n18.desc = 'Secured empowering instructionset';
MERGE (n19:Company {neogen_id: 'c0a0b90ac36ad01977db9745bcb5529a7d2eab8a' })
SET n19.name = 'Raynor-Brekke', n19.desc = 'Secured analyzing budgetarymanagement';
MERGE (n20:Company {neogen_id: 'aaab96ebef4f518c9ea653e65164e77940112b68' })
SET n20.name = 'Altenwerth Group', n20.desc = 'Future-proofed mission-critical paradigm';
MERGE (n21:Country {neogen_id: '5216bca0f48c2da0c282855147a2c5393d722201' })
SET n21.name = 'Pitcairn Islands';
MERGE (n22:Country {neogen_id: '3250895d979d3b1f7d55abe397e7e76375f6477d' })
SET n22.name = 'Dominica';
MERGE (n23:Country {neogen_id: '9ab851f9dea2bf7de8005d118091a0926b9806c8' })
SET n23.name = 'Lithuania';
MERGE (n24:Country {neogen_id: '0ab252dcd77941851ac39040cd18029649552e10' })
SET n24.name = 'Vanuatu';
MERGE (n25:Country {neogen_id: 'de6fd193d9319bf956cbe64d80d2d521be996f93' })
SET n25.name = 'Lithuania';
MATCH (s1:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'}), (e1:Person { neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'})
MERGE (s1)-[edge1:KNOWS]->(e1)
;
MATCH (s2:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'}), (e2:Person { neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'})
MERGE (s2)-[edge2:KNOWS]->(e2)
;
MATCH (s3:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'}), (e3:Person { neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'})
MERGE (s3)-[edge3:KNOWS]->(e3)
;
MATCH (s4:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'}), (e4:Person { neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'})
MERGE (s4)-[edge4:KNOWS]->(e4)
;
MATCH (s5:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'}), (e5:Person { neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'})
MERGE (s5)-[edge5:KNOWS]->(e5)
;
MATCH (s6:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'}), (e6:Person { neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'})
MERGE (s6)-[edge6:KNOWS]->(e6)
;
MATCH (s7:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'}), (e7:Person { neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'})
MERGE (s7)-[edge7:KNOWS]->(e7)
;
MATCH (s8:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'}), (e8:Person { neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'})
MERGE (s8)-[edge8:KNOWS]->(e8)
;
MATCH (s9:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'}), (e9:Person { neogen_id: '904ddce01971764316d6ef36147187750189b95d'})
MERGE (s9)-[edge9:KNOWS]->(e9)
;
MATCH (s10:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'}), (e10:Person { neogen_id: '904ddce01971764316d6ef36147187750189b95d'})
MERGE (s10)-[edge10:KNOWS]->(e10)
;
MATCH (s11:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'}), (e11:Person { neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'})
MERGE (s11)-[edge11:KNOWS]->(e11)
;
MATCH (s12:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'}), (e12:Person { neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'})
MERGE (s12)-[edge12:KNOWS]->(e12)
;
MATCH (s13:Person {neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'}), (e13:Person { neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'})
MERGE (s13)-[edge13:KNOWS]->(e13)
;
MATCH (s14:Person {neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'}), (e14:Person { neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'})
MERGE (s14)-[edge14:KNOWS]->(e14)
;
MATCH (s15:Person {neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'}), (e15:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s15)-[edge15:KNOWS]->(e15)
;
MATCH (s16:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'}), (e16:Person { neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'})
MERGE (s16)-[edge16:KNOWS]->(e16)
;
MATCH (s17:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'}), (e17:Person { neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'})
MERGE (s17)-[edge17:KNOWS]->(e17)
;
MATCH (s18:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'}), (e18:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s18)-[edge18:KNOWS]->(e18)
;
MATCH (s19:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'}), (e19:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s19)-[edge19:KNOWS]->(e19)
;
MATCH (s20:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'}), (e20:Person { neogen_id: '904ddce01971764316d6ef36147187750189b95d'})
MERGE (s20)-[edge20:KNOWS]->(e20)
;
MATCH (s21:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'}), (e21:Person { neogen_id: '904ddce01971764316d6ef36147187750189b95d'})
MERGE (s21)-[edge21:KNOWS]->(e21)
;
MATCH (s22:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'}), (e22:Person { neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'})
MERGE (s22)-[edge22:KNOWS]->(e22)
;
MATCH (s23:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'}), (e23:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s23)-[edge23:KNOWS]->(e23)
;
MATCH (s24:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'}), (e24:Person { neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'})
MERGE (s24)-[edge24:KNOWS]->(e24)
;
MATCH (s25:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'}), (e25:Person { neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'})
MERGE (s25)-[edge25:KNOWS]->(e25)
;
MATCH (s26:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'}), (e26:Person { neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'})
MERGE (s26)-[edge26:KNOWS]->(e26)
;
MATCH (s27:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'}), (e27:Person { neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'})
MERGE (s27)-[edge27:KNOWS]->(e27)
;
MATCH (s28:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'}), (e28:Person { neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'})
MERGE (s28)-[edge28:KNOWS]->(e28)
;
MATCH (s29:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'}), (e29:Person { neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'})
MERGE (s29)-[edge29:KNOWS]->(e29)
;
MATCH (s30:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'}), (e30:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s30)-[edge30:KNOWS]->(e30)
;
MATCH (s31:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'}), (e31:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s31)-[edge31:KNOWS]->(e31)
;
MATCH (s32:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d'}), (e32:Person { neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'})
MERGE (s32)-[edge32:KNOWS]->(e32)
;
MATCH (s33:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d'}), (e33:Person { neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'})
MERGE (s33)-[edge33:KNOWS]->(e33)
;
MATCH (s34:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d'}), (e34:Person { neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'})
MERGE (s34)-[edge34:KNOWS]->(e34)
;
MATCH (s35:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d'}), (e35:Person { neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'})
MERGE (s35)-[edge35:KNOWS]->(e35)
;
MATCH (s36:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'}), (e36:Person { neogen_id: '904ddce01971764316d6ef36147187750189b95d'})
MERGE (s36)-[edge36:KNOWS]->(e36)
;
MATCH (s37:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'}), (e37:Person { neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'})
MERGE (s37)-[edge37:KNOWS]->(e37)
;
MATCH (s38:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'}), (e38:Person { neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'})
MERGE (s38)-[edge38:KNOWS]->(e38)
;
MATCH (s39:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'}), (e39:Person { neogen_id: '904ddce01971764316d6ef36147187750189b95d'})
MERGE (s39)-[edge39:KNOWS]->(e39)
;
MATCH (s40:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'}), (e40:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s40)-[edge40:HAS]->(e40)
;
MATCH (s41:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'}), (e41:Skill { neogen_id: '59fee4dd0cf9395752c075b4965ac59f40246a66'})
MERGE (s41)-[edge41:HAS]->(e41)
;
MATCH (s42:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'}), (e42:Skill { neogen_id: 'b6415157d0146fbdf85d631ab9bd4864c6826549'})
MERGE (s42)-[edge42:HAS]->(e42)
;
MATCH (s43:Person {neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'}), (e43:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s43)-[edge43:HAS]->(e43)
;
MATCH (s44:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'}), (e44:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s44)-[edge44:HAS]->(e44)
;
MATCH (s45:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'}), (e45:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s45)-[edge45:HAS]->(e45)
;
MATCH (s46:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'}), (e46:Skill { neogen_id: 'fe6c7a6df60e3e43939a659a958ba1d8e11d0a7b'})
MERGE (s46)-[edge46:HAS]->(e46)
;
MATCH (s47:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'}), (e47:Skill { neogen_id: 'fe6c7a6df60e3e43939a659a958ba1d8e11d0a7b'})
MERGE (s47)-[edge47:HAS]->(e47)
;
MATCH (s48:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d'}), (e48:Skill { neogen_id: 'b6415157d0146fbdf85d631ab9bd4864c6826549'})
MERGE (s48)-[edge48:HAS]->(e48)
;
MATCH (s49:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'}), (e49:Skill { neogen_id: 'b6415157d0146fbdf85d631ab9bd4864c6826549'})
MERGE (s49)-[edge49:HAS]->(e49)
;
MATCH (s50:Company {neogen_id: 'f80427bf92a435114bbf8ba199fa1eee15e05da6'}), (e50:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s50)-[edge50:LOOKS_FOR_COMPETENCE]->(e50)
;
MATCH (s51:Company {neogen_id: 'f80427bf92a435114bbf8ba199fa1eee15e05da6'}), (e51:Skill { neogen_id: 'fe6c7a6df60e3e43939a659a958ba1d8e11d0a7b'})
MERGE (s51)-[edge51:LOOKS_FOR_COMPETENCE]->(e51)
;
MATCH (s52:Company {neogen_id: 'f80427bf92a435114bbf8ba199fa1eee15e05da6'}), (e52:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s52)-[edge52:LOOKS_FOR_COMPETENCE]->(e52)
;
MATCH (s53:Company {neogen_id: 'f80427bf92a435114bbf8ba199fa1eee15e05da6'}), (e53:Skill { neogen_id: 'fe6c7a6df60e3e43939a659a958ba1d8e11d0a7b'})
MERGE (s53)-[edge53:LOOKS_FOR_COMPETENCE]->(e53)
;
MATCH (s54:Company {neogen_id: '020a8dc29f1f667b2facd86e9dc8aab54bc3fc13'}), (e54:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s54)-[edge54:LOOKS_FOR_COMPETENCE]->(e54)
;
MATCH (s55:Company {neogen_id: '020a8dc29f1f667b2facd86e9dc8aab54bc3fc13'}), (e55:Skill { neogen_id: '59fee4dd0cf9395752c075b4965ac59f40246a66'})
MERGE (s55)-[edge55:LOOKS_FOR_COMPETENCE]->(e55)
;
MATCH (s56:Company {neogen_id: '020a8dc29f1f667b2facd86e9dc8aab54bc3fc13'}), (e56:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s56)-[edge56:LOOKS_FOR_COMPETENCE]->(e56)
;
MATCH (s57:Company {neogen_id: '020a8dc29f1f667b2facd86e9dc8aab54bc3fc13'}), (e57:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s57)-[edge57:LOOKS_FOR_COMPETENCE]->(e57)
;
MATCH (s58:Company {neogen_id: '8d2774b8d3445fda288d9d5e7490a3069a85802c'}), (e58:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s58)-[edge58:LOOKS_FOR_COMPETENCE]->(e58)
;
MATCH (s59:Company {neogen_id: '8d2774b8d3445fda288d9d5e7490a3069a85802c'}), (e59:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s59)-[edge59:LOOKS_FOR_COMPETENCE]->(e59)
;
MATCH (s60:Company {neogen_id: '8d2774b8d3445fda288d9d5e7490a3069a85802c'}), (e60:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s60)-[edge60:LOOKS_FOR_COMPETENCE]->(e60)
;
MATCH (s61:Company {neogen_id: '8d2774b8d3445fda288d9d5e7490a3069a85802c'}), (e61:Skill { neogen_id: 'fe6c7a6df60e3e43939a659a958ba1d8e11d0a7b'})
MERGE (s61)-[edge61:LOOKS_FOR_COMPETENCE]->(e61)
;
MATCH (s62:Company {neogen_id: 'c0a0b90ac36ad01977db9745bcb5529a7d2eab8a'}), (e62:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s62)-[edge62:LOOKS_FOR_COMPETENCE]->(e62)
;
MATCH (s63:Company {neogen_id: 'c0a0b90ac36ad01977db9745bcb5529a7d2eab8a'}), (e63:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s63)-[edge63:LOOKS_FOR_COMPETENCE]->(e63)
;
MATCH (s64:Company {neogen_id: 'c0a0b90ac36ad01977db9745bcb5529a7d2eab8a'}), (e64:Skill { neogen_id: 'b6415157d0146fbdf85d631ab9bd4864c6826549'})
MERGE (s64)-[edge64:LOOKS_FOR_COMPETENCE]->(e64)
;
MATCH (s65:Company {neogen_id: 'c0a0b90ac36ad01977db9745bcb5529a7d2eab8a'}), (e65:Skill { neogen_id: '0763e4e108132acc614319b63511247d06555da5'})
MERGE (s65)-[edge65:LOOKS_FOR_COMPETENCE]->(e65)
;
MATCH (s66:Company {neogen_id: 'aaab96ebef4f518c9ea653e65164e77940112b68'}), (e66:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s66)-[edge66:LOOKS_FOR_COMPETENCE]->(e66)
;
MATCH (s67:Company {neogen_id: 'aaab96ebef4f518c9ea653e65164e77940112b68'}), (e67:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s67)-[edge67:LOOKS_FOR_COMPETENCE]->(e67)
;
MATCH (s68:Company {neogen_id: 'aaab96ebef4f518c9ea653e65164e77940112b68'}), (e68:Skill { neogen_id: '65291fbdc3c6e20e41aaddb660fbd71fd60d081a'})
MERGE (s68)-[edge68:LOOKS_FOR_COMPETENCE]->(e68)
;
MATCH (s69:Company {neogen_id: 'aaab96ebef4f518c9ea653e65164e77940112b68'}), (e69:Skill { neogen_id: '59fee4dd0cf9395752c075b4965ac59f40246a66'})
MERGE (s69)-[edge69:LOOKS_FOR_COMPETENCE]->(e69)
;
MATCH (s70:Company {neogen_id: 'f80427bf92a435114bbf8ba199fa1eee15e05da6'}), (e70:Country { neogen_id: '9ab851f9dea2bf7de8005d118091a0926b9806c8'})
MERGE (s70)-[edge70:LOCATED_IN]->(e70)
;
MATCH (s71:Company {neogen_id: '020a8dc29f1f667b2facd86e9dc8aab54bc3fc13'}), (e71:Country { neogen_id: '9ab851f9dea2bf7de8005d118091a0926b9806c8'})
MERGE (s71)-[edge71:LOCATED_IN]->(e71)
;
MATCH (s72:Company {neogen_id: '8d2774b8d3445fda288d9d5e7490a3069a85802c'}), (e72:Country { neogen_id: 'de6fd193d9319bf956cbe64d80d2d521be996f93'})
MERGE (s72)-[edge72:LOCATED_IN]->(e72)
;
MATCH (s73:Company {neogen_id: 'c0a0b90ac36ad01977db9745bcb5529a7d2eab8a'}), (e73:Country { neogen_id: 'de6fd193d9319bf956cbe64d80d2d521be996f93'})
MERGE (s73)-[edge73:LOCATED_IN]->(e73)
;
MATCH (s74:Company {neogen_id: 'aaab96ebef4f518c9ea653e65164e77940112b68'}), (e74:Country { neogen_id: '9ab851f9dea2bf7de8005d118091a0926b9806c8'})
MERGE (s74)-[edge74:LOCATED_IN]->(e74)
;
MATCH (s75:Person {neogen_id: '6f8d2db5b1a435850a0fc5eabd9fad0f3009c3b9'}), (e75:Country { neogen_id: '9ab851f9dea2bf7de8005d118091a0926b9806c8'})
MERGE (s75)-[edge75:LIVES_IN]->(e75)
;
MATCH (s76:Person {neogen_id: '50556e45b1dd5ac95939e2652d6fbb611310571f'}), (e76:Country { neogen_id: 'de6fd193d9319bf956cbe64d80d2d521be996f93'})
MERGE (s76)-[edge76:LIVES_IN]->(e76)
;
MATCH (s77:Person {neogen_id: 'b9eceedc17f856d292d58ec849e2071b10d42868'}), (e77:Country { neogen_id: '5216bca0f48c2da0c282855147a2c5393d722201'})
MERGE (s77)-[edge77:LIVES_IN]->(e77)
;
MATCH (s78:Person {neogen_id: 'c183e973c699d9abaa6951ea017fc693ffdb10b3'}), (e78:Country { neogen_id: '3250895d979d3b1f7d55abe397e7e76375f6477d'})
MERGE (s78)-[edge78:LIVES_IN]->(e78)
;
MATCH (s79:Person {neogen_id: '4aa1a4cd5016373de87abe7831dd28ceb4343533'}), (e79:Country { neogen_id: 'de6fd193d9319bf956cbe64d80d2d521be996f93'})
MERGE (s79)-[edge79:LIVES_IN]->(e79)
;
MATCH (s80:Person {neogen_id: '4f4e0a2335b31533c29c6f570166c735b97c6f1f'}), (e80:Country { neogen_id: '9ab851f9dea2bf7de8005d118091a0926b9806c8'})
MERGE (s80)-[edge80:LIVES_IN]->(e80)
;
MATCH (s81:Person {neogen_id: 'f6ada6283be128f9d200f005d0e6c80fb34309af'}), (e81:Country { neogen_id: '0ab252dcd77941851ac39040cd18029649552e10'})
MERGE (s81)-[edge81:LIVES_IN]->(e81)
;
MATCH (s82:Person {neogen_id: 'c68a47ccae7882de04a6314d696ddb6539371939'}), (e82:Country { neogen_id: '3250895d979d3b1f7d55abe397e7e76375f6477d'})
MERGE (s82)-[edge82:LIVES_IN]->(e82)
;
MATCH (s83:Person {neogen_id: '904ddce01971764316d6ef36147187750189b95d'}), (e83:Country { neogen_id: '5216bca0f48c2da0c282855147a2c5393d722201'})
MERGE (s83)-[edge83:LIVES_IN]->(e83)
;
MATCH (s84:Person {neogen_id: '52c2bedcfd2bd7f1760386412091c3e0af767226'}), (e84:Country { neogen_id: '3250895d979d3b1f7d55abe397e7e76375f6477d'})
MERGE (s84)-[edge84:LIVES_IN]->(e84)
;
MATCH (n1:Person) REMOVE n1.neogen_id;
MATCH (n2:Skill) REMOVE n2.neogen_id;
MATCH (n3:Company) REMOVE n3.neogen_id;
MATCH (n4:Country) REMOVE n4.neogen_id;
